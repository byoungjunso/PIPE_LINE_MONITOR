#!/usr/bin/env python
# coding: utf-8

from influxdb import DataFrameClient
from influxdb import InfluxDBClient
import pandas as pd
### select 에서 함수까지 같이 부르려 한다면 __init__에 dbHost, ~ 등등과 sql 을 같이 받고 select 메소드를 분리하여 sql만 상속받으면 된다
class influxDB_C:
    def __init__(self, dbHost, dbPort, dbUser, dbPwd, dbName) :
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbUser = dbUser
        self.dbPwd = dbPwd
        self.dbName = dbName
        
    def dbSelect(self, sql) :
        client = DataFrameClient(self.dbHost, self.dbPort, self.dbUser, self.dbPwd, self.dbName)
        q_result = client.query(sql)
        q_result = pd.concat(q_result, axis=1)
        q_result = q_result.tz_convert('Asia/Seoul')
        return (q_result)
    
    def dbSelectParm(self, sql, bind_params) :
        client = InfluxDBClient(self.dbHost, self.dbPort, self.dbUser, self.dbPwd, self.dbName)
        q_result = client.query(sql, bind_params=bind_params)
        q_result = pd.DataFrame(q_result.get_points())
        q_result['time'] = pd.to_datetime(q_result['time']).dt.tz_convert('Asia/Seoul')
        q_result = q_result.set_index(keys='time')
        q_result.rename_axis(None, axis=0, inplace=True)
        return (q_result)
