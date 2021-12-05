#!/usr/bin/env python
# coding: utf-8

import datetime
import functools

import pandas as pd

from .module import all_delete_insert
from .module import delete_upsert
from .module import create_table
from .module import db_module
from .pyjin import pyjin

def get_mode(df_from,
             acc_to,              
             db_to,              
             table_to, 
             primary_key: str):
    
    ## to 테이블이 존재하지 않을경우
    if not pyjin.check_is_table(acc=acc_to, 
                                table_name = table_to,
                                schema_name = db_to):
        
        return 'no_table'   
    
    columns_df_from = df_from.columns.tolist()        
        
    if primary_key is None:            
        return primary_key
    elif primary_key in columns_df_from and 'update_date' in columns_df_from:
        return '{},update_date'.format(primary_key)        
    elif primary_key in columns_df_from:
        return primary_key

def main(input_dict, 
          sync_info: dict,          
          *args,
          **kwargs):
    
    # 기본적으로 pandas data frame으로 변경해서 처리
    df = pd.DataFrame([input_dict])
    
    ## 테이블이 존재하지 않을경우 생성
    if not pyjin.check_is_table(acc=sync_info['acc_to'],
                                table_name = sync_info['table_to'],
                                schema_name = sync_info['db_to']):
        
        # schema 정의가 없을경우 판다스 to_sql로 생성
        if sync_info['create_table_schema'] not in sync_info.keys():
            with pyjin.connectDB(**sync_info['acc_to']) as con:            
                df.to_sql(sync_info['table_to'],
                            schema=sync_info['db_to'],
                            con=con,
                            if_exists='replace',
                            index=False,
                            chunksize=1000,
                            method='multi')

    # all delete all insert case
    elif sync_info['update_method'] == 'delete_all':
        all_delete_insert.delete_all_insert_all(acc_to = sync_info['acc_to'],
                                                df=df,
                                                table_to = sync_info['table_to'],
                                                db_to = sync_info['db_to'])                
        pyjin.print_logging('delete all data and insert all data')  
        
                    
    elif sync_info['update_method'] == 'update':
        mode = get_mode(df_from = df,
                        acc_to = sync_info['acc_to'],                        
                        db_to = sync_info['db_to'],                        
                        table_to=sync_info['table_to'],
                        primary_key = sync_info['primary_key'])        
        
        delete_upsert.Main_by_json(acc_to = sync_info['acc_to'],
                                   db_to = sync_info['db_to'],
                                   table_to = sync_info['table_to'],
                                   primary_key = sync_info['primary_key'],
                                   column_matching_method = sync_info['column_matching_method'],
                                   df_from = df,
                                   mode = mode)                                   
                                   
        print('upsert completed')
        
    return True
        

if __name__ == '__main__':
    '''
    example code
    

    input_dict = [{'id':1, 'col':'test'},]
    sync_info = {        
        'acc_to' : , # host address you want to syncronize
        'db_to' : 'db_name', # schema name
        'table_to' : 'table_name',
        'primary_key' : 'id', # name of primary key column
        'update_method' : 'update', # options : update, delete_all 
        'create_table_schema' : None, # if this is none, you can 
        'column_matching_method' : 'to', # which column you want to sync if input and output are different
    }

    main(list_input_dict = list_input_dict, list_sync_info = list_sync_info)

    '''