# coding: utf-8
import datetime

import pandas as pd
import numpy as np

from ..pyjin import pyjin
from . import db_module

def delete_all_insert_all(acc_to, df ,table_to, db_to):
    with pyjin.connectDB(**acc_to, engine_type='NullPool') as con:
        try:                
            ## transaction
            with con.begin():
                ## delete all data
                ## release foreignkey constrains when deleting tables and restore
                pyjin.execute_query(con,"SET foreign_key_checks = 0")
                pyjin.execute_query(con, 'delete from {}.{}'.format(db_to, table_to))        
                
                # write data
                # 큰 query 처리 불가능한 경우를 위해 chunksize 추가
                df.to_sql(table_to, con=con, if_exists='append', index=False, chunksize=5000, method='multi', schema=db_to)                               
                pyjin.execute_query(con,"SET foreign_key_checks = 1")                     
        except Exception as e:
            pyjin.print_logging("{} delete and write failed, {}".format(table_to, e))            
    return True

def main(acc_from, 
         acc_to, 
         db_from, 
         db_to, 
         table_from, 
         table_to,
         col_matching):  
    
    '''
    service server 에서 데이터 가져오기
    '''
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con_from:
        df_from = db_module.read_sqldata(con_from, db_from, table_from)
                
    columns_bring = db_module.get_col_matched(
        acc_from = acc_from,
        acc_to = acc_to, 
        db_from = db_from, 
        db_to = db_to, 
        table_from = table_from, 
        table_to = table_to, 
        column_matching_method= col_matching    
        )
        
    df_from = df_from[columns_bring]
    delete_all_insert_all(acc_to, df_from, table_to, db_to)

    return True
        