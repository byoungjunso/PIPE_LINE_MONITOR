#!/usr/bin/env python
# coding: utf-8

import os
import time
import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np
from influxdb import DataFrameClient
import datetime
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
pd.set_option('display.max_columns', 14)

app_dir = "${APP_DIR}"
tmp_dir = app_dir + "/sincedb"
log_dir = app_dir + "/logs"
log_file = log_dir + "/spark_strm_monitor.log"
target_directory = [tmp_dir, log_dir]
lt_file = tmp_dir + "/last_time.db"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=20240000, backupCount=3)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def default_setup(directory, file): 
    try:
        for path in directory:
            if not os.path.exists(path):
                os.makedirs(path)
    except OSError:
        print("ERROR: Failed to create the directory")
    try:
        if not os.path.exists(file):
            open(file, 'w')
    except OSError:
        print("ERROR: Failed to create the file")
    global sq_conn
    sq_conn = sqlite3.connect(lt_file)
    global sq_cursor
    sq_cursor  = sq_conn.cursor()
    sq_cursor.execute("CREATE TABLE IF NOT EXISTS last_time(id TEXT, app_name TEXT, last_time TEXT )")

default_setup(target_directory, lt_file)

#"haState":"ACTIVE"
rm1_host = 'http://${RESOURCE_MANAGER1_IP:PORT}'
rm2_host = 'http://${RESOURCE_MANAGER2_IP:PORT}'



def get_rma(rm1, rm2):
    for rm in [rm1, rm2]:
        rma_chk_url = rm + "/ws/v1/cluster/info"
        try:
            rm_info = requests.get(rma_chk_url, timeout=1)
            rm_info_json = json.loads(rm_info.text)
            rm_result = rm_info_json['clusterInfo']['haState']
            if rm_result == 'ACTIVE':
                rma = rm
                return rma
        except requests.exceptions.Timeout as e:
            #print (e)
            logger.warning("rm1, rm2 Timeout Exception")


def get_from_time(timeInterval):
    return(datetime.datetime.utcnow() - datetime.timedelta(minutes=timeInterval))


def get_spark_metric(app_id, app_name, app_url):
    sq_cursor.execute("SELECT * FROM last_time ").fetchall()
    col_name = ['index', 'appName', 'last_time']
    sq_last_time_df = pd.DataFrame(sq_cursor.execute("SELECT * FROM last_time ").fetchall(), columns = col_name)
    sq_last_time_df.set_index('index',drop=True)
    try:
        url = app_url + "/api/v1/applications/" + app_id + "/streaming/batches?status=COMPLETED"
        
        r = requests.get(url)
        if r.status_code == 200:
            g = json.loads(r.text)
            res = json_normalize(g)
            retval = res
            retval.insert(0,'appName', app_name)
            retval['batchTime'] = retval['batchTime'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fGMT'))
            
            try:
                old_from_time = sq_last_time_df[sq_last_time_df['appName'] == app_name]['last_time']
                #return(retval[retval['batchTime'] > old_from_time.item()])
                return(retval[retval['batchTime'] > old_from_time.iloc[0]])
            except:
                return(retval[retval['batchTime'] > get_from_time(6)])
    except:
         logger.warning("ERROR: Failed get to spark metric")
         fail_app = app_id, app_name, app_url
         logger.warning(fail_app)

            
# app ID 및 app name, tracking URI 추출 후 각각에 대한 metric 추출
def get_spark_applist():
    host = get_rma(rm1_host, rm2_host)
    url = host + "/ws/v1/cluster/apps?state=RUNNING&applicationTypes=SPARK"
    app_q = requests.get(url).json()
    app_q_json = json.dumps(app_q, indent=1)
    app_q_json = json.loads(app_q_json)
    
    ### json 에서 꺼내서 맵핑
    try:
        result = app_q_json['apps']
        applist = list(map(lambda app:get_spark_metric(app['id'],app['name'],app['trackingUrl']), result['app']))
        result_app_list = pd.concat(applist, sort=True)
        clone_app_list = pd.concat(applist, sort=True)
        logger.info("----------raw_result_app_list-------------")
        logger.info(clone_app_list)
        if 'firstFailureReason' in result_app_list.columns:
            col_list = ['appName', 'batchDuration', 'batchId', 'batchTime', 'inputSize', 'numFailedOutputOps', 'numTotalOutputOps', 'processingTime', 'schedulingDelay', 'status', 'totalDelay']
            result_app_list = result_app_list[col_list]
            return(result_app_list)
        else:
            col_list = ['appName', 'batchDuration', 'batchId', 'batchTime', 'inputSize', 'numFailedOutputOps', 'numTotalOutputOps', 'processingTime', 'schedulingDelay', 'status', 'totalDelay']
            result_app_list = result_app_list[col_list]
            return(result_app_list)
    except:
        logger.warning("WARN: Get Spark App List Fail")

#############################################################################################
# 불러온 app의 metric에 대해서 마지막 시간 데이터만 남기는 함수
def get_last_time_data(ddf):
    # appName별로 그룹 바이 해서, 가장 마지막 시간 데이터만 남긴 dataframe
    ddfMaxTime = ddf.groupby(['appName']).agg(batchTime=('batchTime', np.max))
    # 원본 데이터 프레임과, 방금 만든 app별 마지막 시간만 남긴 데이터 프레임을 이너 조인시킨다
    # 이 때 조인 조건은, 앱 이름과 배치 시간
    # 그렇게 되면, 원본 데이터 프레임에서 앱 별로 가장 마지막 배치 시간의 레코드만 남는다
    last_time_data = pd.merge(left=ddf, right=ddfMaxTime, how='inner', on=['appName', 'batchTime'])
    return last_time_data

def last_update(result_df):
    df_last = get_last_time_data(result_df[['appName','batchTime']])
    if not len(df_last) == 0:
        df_last.to_sql('last_time', sq_conn, if_exists='replace')

def main():
    while True:
        result_df=get_spark_applist()
        result_to_influxdb_set = result_df.set_index("batchTime")
        client = DataFrameClient('90.90.220.38', '8086', 'admin', 'qnstjr89!', 'SPARK_DB')
        client.create_database('SPARK_DB')
        datatags = ['appName', 'status']
        logger.info("----------influxdb_insert_data-------------")
        logger.info(result_to_influxdb_set)
        client.write_points(result_to_influxdb_set, 'short_term', tag_columns=datatags,protocol='line')
        last_update(result_df)
        time.sleep(30)

if __name__ == "__main__":
    main()
