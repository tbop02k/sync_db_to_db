import logging

import pandas as pd
import datetime
import json
from dateutil.relativedelta import relativedelta

from sqlalchemy import create_engine, text            
from sqlalchemy.pool import Pool, NullPool
from sqlalchemy import create_engine, text
import numpy as np
import psycopg2

pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_columns',None)
pd.set_option('display.max_colwidth', 1000)
pd.set_option('display.min_rows', 100)
pd.set_option('display.max_rows', 1000)

def pickle_save(object, path):
    import pickle
    with open(path,'wb') as f:
        pickle.dump(object,f)
    return True

def pickle_load(path):
    import pickle
    with open(path,'rb') as f:
        res=pickle.load(f)
    return res

def dict_rename(in_dict,map_dict):
    for key, value in in_dict.items():
        if key in map_dict.keys():
            in_dict[map_dict[key]]=value
            del in_dict[key]
    return in_dict

def from_now(years,months,days):
    return datetime.datetime.now() + relativedelta(years=years)+relativedelta(months=months)+relativedelta(days=days)

#### database related
def insert_query(schema, table_name, input_json):
    columns=','.join(["`"+i+"`" for i in input_json.keys()])
    values=','.join(['"'+str(i)+'"' for i in input_json.values()])
    query="INSERT INTO {}.{} ( {} ) values ( {} )".format(schema, table_name, columns, values)
    return query

def upsert_query(schema, table_name, columns):
    columns_joined = ','.join(["`" + i + "`" for i in columns])
    values_joined = ','.join([':' + str(i) for i in columns])
    update_state=','.join([str(column) + '= values(' +str(column) + ')' for column in columns])
    query="insert INTO {}.{} ( {} ) values ( {} ) on duplicate key update {}".format(schema, table_name, columns_joined, values_joined, update_state)
    return query

'''
How to use
pyjin.conn_exec_close(anal_acc, upsert_query(schema, table_name, best_result.keys()), **best_result)  
'''

def set_primary_key_query(table_schema, table_name, primary_key):
    return 'ALTER TABLE {}.{} ADD PRIMARY KEY({})'.format(table_schema, table_name, primary_key)

# table check query
def check_is_table(acc, table_name, schema_name, column_list=None):
    try:
        conn_exec_close(acc,
                        """
                        SELECT 1 FROM {schema_name}.{table_name} LIMIT 1
                        """.format(schema_name=schema_name, table_name=table_name))        
                
        df = conn_exec_close(acc,
                                 """
                                SELECT `COLUMN_NAME` 
                                FROM `INFORMATION_SCHEMA`.`COLUMNS` 
                                WHERE `TABLE_SCHEMA`="{schema_name}"
                                    AND `TABLE_NAME`="{table_name}";
                                """.format(schema_name= schema_name, table_name=table_name),
                                 output='df')
        if column_list is not None:
            if not set(column_list).issubset(set(df['COLUMN_NAME'].tolist())):
                raise Exception()
            
        print("table {} exists".format(table_name))
        return True
    except BaseException as e:
        print("table {} doesn't exist".format(table_name))
        return False

def jin_df_iter(df):
    for idx,row in df.iterrows():
        row=dict(row)
        row.update({key:None if pd.isnull(value) else value for key, value in row.items()})
        yield row

def jin_df_json(df):    
    '''
    df를 json 으로 바꾼후 모든 np.nan np.inf np.naT 등의 value를 None으로 바꿈    
    '''
    json_list=df.replace([np.inf, -np.inf], np.nan).to_dict(orient='records')    
    for row in json_list:
        row.update({key:None if pd.isnull(value) else value for key, value in row.items()})
    return json_list

def get_days_gap( end_date, start_date, listHolidays, except_type=1):
    if except_type==1: #type 1 일경우 business days count
        listGapDates = pd.Series(pd.date_range(start=start_date,
                                          end=end_date,
                                          freq='B')).tolist()
    else:#type 1 일경우 all days count
        listGapDates = pd.Series(pd.date_range(start=start_date,
                                          end=end_date,
                                          freq='D')).tolist()
    listGapDates = sorted(listGapDates)
    listBusinessGapDates = sorted(list(set(listGapDates) - set(listHolidays)))
    return len(listBusinessGapDates)


def print_logging(contents):
    '''
    print 와 loggin 을 둘다 하는 함수
    '''
    print(contents)
    logging.info(contents)
    return True
          
            
            
def connectDB(host, port, user, password, db, engine_type='None',dbtype='mysql+pymysql'):    
    connect_args = {}
    if dbtype == 'mysql+pymysql':
        connect_args={'ssl':{'fake_flag_to_enable_tls': True}}   
        db = '{dbtype}://{user}:{password}@{host}:{port}/{db}?charset=utf8&ssl=true'.format(dbtype='mysql+pymysql', host=host, user=user, port=port, password=password, db=db)
    
    else:
        db = '{dbtype}://{user}:{password}@{host}:{port}/{db}'.format(dbtype='postgresql+psycopg2', host=host, user=user, port=port, password=password, db=db)

    if engine_type=='NullPool':
        engine = create_engine(db, connect_args=connect_args, poolclass=NullPool, pool_recycle=100)
    else:
        #default poolclass = QueuePool
        engine = create_engine(db, pool_recycle=100, connect_args=connect_args)
    conn = engine.connect()
    return conn

### exception이 어느 단계에서 발생을 했는가.
def conn_exec_close(connectInfo, query, output=None, is_return=True,**kwargs):
    with connectDB(**connectInfo) as conn:
        try:
            res = conn.execute(text(query), kwargs)

            if is_return:
                if output is None:
                    return res.fetchall(), res.keys()
                elif output=='df':
                    res = pd.DataFrame(res.fetchall(), columns=res.keys())
                    return res
            else:
                return True
        except ResourceWarning:
            return True
        except BaseException as e:
            raise e

def execute_query(conn,query,output=None, is_return=True, **kwargs):
    try:
        res = conn.execute(text(query), kwargs)

        if is_return:
            if output== 'df':
                res = pd.DataFrame(res.fetchall(), columns=res.keys())
            return res
        else:
            return True
    except Exception as e:
        print('query error : ', e)
        raise e
