#!/usr/bin/env python
# coding: utf-8


import os
import sys
from pathlib import Path
import argparse
import logging
from logging.handlers import RotatingFileHandler
import yaml
import pandas as pd
import datetime
from pandas.api.types import is_datetime64_ns_dtype
import numpy as np
from influxdb import DataFrameClient
import logging
from logging.handlers import RotatingFileHandler
from pytz import timezone
import signal 

SHUTDOWN = False
def shutdown(sigNum, frame) :
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write('Catch Signal : %s' % sigNum)
	sys.stderr.flush()

# signal 
signal.signal(signal.SIGTERM, shutdown)		 # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)		  # sigNum  2 : Interrupt
try : signal.signal(signal.SIGHUP, shutdown)	# sigNum  1 : HangUp
except : pass
try : signal.signal(signal.SIGPIPE, shutdown)   # sigNum 13 : Broken Pipe
except : pass


app_dir = os.path.dirname(os.path.abspath(__file__))
hist_dir = app_dir + "/hist"
log_dir = app_dir + "/logs"
conf_dir = app_dir + "/conf"
lib_dir = app_dir + "/lib"
target_dir = [hist_dir, log_dir, conf_dir, lib_dir]

def createDirectory(directory):
    try:
        for path in directory:
             if not os.path.exists(path):
                os.makedirs(path)
    except OSError:
         print("Error: Failed to create the directory.")

createDirectory(target_dir)

log_file = log_dir + "/Data_Checker.log"
logger = logging.getLogger()
logger.setLevel(logging.WARNING)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=20240000, backupCount=3)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
sys.path.append(lib_dir)

import SmsClient
from influxdb_conn_v2 import influxDB_C
with open(conf_dir + '/default.yml') as config_file:
    configs = yaml.safe_load(config_file)

def select_influxdb(query, bind_params=None):
    dbHost = configs['INFLUXDB']['host']
    dbPort = configs['INFLUXDB']['port']
    dbUser = configs['INFLUXDB']['user']
    dbPwd = configs['INFLUXDB']['password']
    dbName = configs['INFLUXDB']['db']
    try:
        conn = influxDB_C(dbHost, dbPort, dbUser, dbPwd, dbName)
    except:
        logger.error("Can not collect INFLUXDB")

    if bind_params is None:
        try:
            result = conn.dbSelect(query)
            return (result)
        except:
            return (pd.DataFrame())
            logger.error("Can not %s" %query)
    else:
        try:
            result = conn.dbSelectParm(query, bind_params)
            return (result)
        except Exception as e:
            print ("ERROR: Query result is empty!! Check your Query({query}) in {exception} condition.".format(query = query, exception=e))
            logger.error("Can not %s" %query)


def get_analysis_rate(target_time):
    subject = 'analysis_rate'
    if configs['CHECK_LTE_ANALYSYS_RATE']['use_yn'] == 'use':
        bind_params = {'EVENT_TIME': target_time}
        try:
            # 벤더별 전체 ENB ID 개수를 구하고, 혹시 수집되지 않는 벤더 확인
            result_all_cnt = select_influxdb(configs['CHECK_LTE_ANALYSYS_RATE']['query1'], bind_params)
            vend_base = pd.DataFrame(['NSN', 'SS', 'ELG'], columns=['VEND_ID'])
            result_all_cnt = pd.merge(left = vend_base, right = result_all_cnt, how= "left", on = 'VEND_ID')
            # 분석중인 RRC, ASR, CSL 개수 확인
            result_call_log_cnt = select_influxdb(configs['CHECK_LTE_ANALYSYS_RATE']['query2'], bind_params)
            # type_cnt_by_enb 는 EVENT_TIME 이 TAG로 되어있어서, EVENT_TIME으로 컬럼명 변경
            result_call_log_cnt.rename(columns = {'EVENT_TIME_TAG':'EVENT_TIME'}, inplace=True)
            # 벤더별 type_cnt_by_enb join
            result = pd.merge(left = result_all_cnt, right = result_call_log_cnt, how = "left", on = ['VEND_ID', 'EVENT_TIME'])
            result.insert(0, 'subject', subject)
            return (result)
        except:
            logger.error("Can not collect INFLUXDB")


def check_metric(subject_v):
    if subject_v == 'lte_analysis_rate' :
        # 현재 분석율과 직전 분석율 비교를 위해 scope 설정
        now = datetime.datetime.now()
        last_time = now - datetime.timedelta(hours=2)
        last_time = last_time.strftime("%Y%m%d%H000000")
        past_time = now - datetime.timedelta(hours=3)
        past_time = past_time.strftime("%Y%m%d%H000000")
        # 직전 데이터 추출
        an_ago = get_analysis_rate(last_time)
        # 직전 시간 이전 데이터 추출
        two_ago = get_analysis_rate(past_time)

        # 결측치 처리
        an_ago['NULL_YN'] = np.where(pd.isnull(an_ago['ALL_CNT']) == True, 'Y', 'N')
        two_ago['NULL_YN'] = np.where(pd.isnull(two_ago['ALL_CNT']) == True, 'Y', 'N')

        # 비교 데이터 처리
        summary_df = pd.concat([an_ago, two_ago], ignore_index=True, sort=False)
        summary_df.loc[:, 'FAIL_RATE'] = np.where(summary_df['subject'] != 'analysis_rate', 'None',
                                        np.where(summary_df['VEND_ID'] == 'NSN', round(100 - (summary_df['RRC_EXIST_ENB']/summary_df['ALL_CNT']) * 100, 2),
                                            np.where(summary_df['VEND_ID'] == 'SS', round(100 - (summary_df['CSL_EXIST_ENB']/summary_df['ALL_CNT']) * 100, 2),
                                                np.where(summary_df['VEND_ID'] == 'ELG', round(100 - (summary_df['ASR_EXIST_ENB']/summary_df['ALL_CNT']) * 100, 2),
                                                'None'))))
        summary_df = summary_df[['subject', 'VEND_ID', 'EVENT_TIME', 'NULL_YN', 'FAIL_RATE']]
        summary_df = summary_df.astype({'FAIL_RATE' : 'float'})

        # loop in list 를 위해 빈  DataFrame 생성
        result_df = pd.DataFrame()
        vend_list = ['NSN', 'SS', 'ELG']

        for vend_id in vend_list:
            null_check = summary_df.query('VEND_ID == "{vend_id}" & EVENT_TIME == "{query_time}"'.format(vend_id = vend_id, query_time=last_time)).empty # 직전 시간 데이터 결측 여부 확인

            if not null_check:
                # 분석 실패율 비교값 산출
                past_df = summary_df.query('VEND_ID == "{vend_id}" & EVENT_TIME == "{query_time}"'.format(vend_id = vend_id, query_time=past_time))
                last_df = summary_df.query('VEND_ID == "{vend_id}" & EVENT_TIME == "{query_time}"'.format(vend_id = vend_id, query_time=last_time))
                diff = round(last_df.iloc[0]['FAIL_RATE'] - past_df.iloc[0]['FAIL_RATE'], 2)
                # 비교값 VS 임계치 (5% 이상)
                if diff < 5.0:
                    subject = subject_v
                    vend_id = vend_id
                    target_time = datetime.datetime.strptime(last_time, '%Y%m%d%H%M0000').strftime('%Y-%m-%d %H:%M')
                    diff_rate = '{:.2%}'.format(diff/100)
                    result = 'OK'
                    result = pd.DataFrame({'subject': [subject], 'vend_id': [vend_id], 'target_time': [target_time], 'diff_rate': [diff_rate], 'result': [result]})
                    result_df = result_df.append(result)
                else:
                    subject = subject_v
                    vend_id = vend_id
                    target_time = datetime.datetime.strptime(last_time, '%Y%m%d%H%M0000').strftime('%Y-%m-%d %H:%M')
                    diff_rate = '{:.2%}'.format(diff/100)
                    result = 'NOK'
                    result = pd.DataFrame({'subject': [subject], 'vend_id': [vend_id], 'target_time': [target_time], 'diff_rate': [diff_rate], 'result': [result]})
                    result_df = result_df.append(result)
            else:
                subject = subject_v
                vend_id = vend_id
                target_time = datetime.datetime.strptime(last_time, '%Y%m%d%H%M0000').strftime('%Y-%m-%d %H:%M')
                diff_rate = '최근 데이터 없음'
                result = 'NOK'
                result = pd.DataFrame({'subject': [subject], 'vend_id': [vend_id], 'target_time': [target_time], 'diff_rate': [diff_rate], 'result': [result]})
                result_df = result_df.append(result)

        result_df['receiver'] = configs['CHECK_LTE_ANALYSYS_RATE']['sms_receiver']
        return (result_df)


def history_check(filename):
    if not os.path.isfile(filename) or os.path.getsize(filename) == 0:
        with open(filename, "w") as f:
            f.write('OK')

    with open(filename, "r") as f:
        contents = f.read()
        return(contents)

def monit_with_sms(subject_v):
    sms = SmsClient.SmsClient()

    if subject_v == 'lte_analysis_rate' :
        alarm_df = check_metric(subject_v)

        for index, row in alarm_df.iterrows():
            filename = hist_dir + '/' + row['subject'] + '.' + row['vend_id']
            history = history_check(filename)

            if row['result'] == 'NOK':
                if history == 'OK':
                    with open(filename, 'w') as file:
                        file.write('NOK')
                    msg = '[발생][{vend_id} 분석율 알람]\n대상시간:{target_time}\n직전 시간 diff: {diff_rate}'.format(vend_id = row['vend_id'], target_time=row['target_time'], diff_rate=row['diff_rate'])
                    for receiver in row['receiver'].replace(' ', '').split(','):
                        sms.send(receiver, msg)

            else:
                if history == 'NOK':
                    with open(filename, 'w') as file:
                        file.write('OK')
                    msg = '[해제][{vend_id} 분석율 알람]\n대상시간:{target_time}\n직전 시간 diff: {diff_rate}'.format(vend_id = row['vend_id'], target_time=row['target_time'], diff_rate=row['diff_rate'])
                    for receiver in row['receiver'].replace(' ', '').split(','):
                        sms.send(receiver, msg)


def main():
    monit_with_sms('lte_analysis_rate')

main()
