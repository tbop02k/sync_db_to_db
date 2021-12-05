import pandas as pd

from ..pyjin import pyjin

def read_sqldata(con_from, 
                 db_from, 
                 table_from):
            
    query='''
        select * from {}.{}
    '''.format(db_from, table_from)    
    
    print("reading {}.{} ".format(db_from, table_from))    
    try:
        pyjin.execute_query(con_from,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        df = pyjin.execute_query(con_from, query, output='df')
        pyjin.execute_query(con_from,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        print('completed')        
    except Exception as e:
        print(e)
        raise Exception("table read failed")
                    
    return df

def get_col_matched(acc_from,
                    acc_to, 
                    db_from, 
                    db_to, 
                    table_from, 
                    table_to, 
                    column_matching_method):
    
    ## bring all columns of the table_to
    columns_df_to = pyjin.conn_exec_close(acc_to,
                                """
                                select column_name
                                from INFORMATION_SCHEMA.COLUMNS
                                where TABLE_NAME=:table and TABLE_SCHEMA =:db
                                """, 
                                db=db_to, 
                                table=table_to, 
                                output='df')['column_name'].tolist()        
    
    columns_df_from = pyjin.conn_exec_close(acc_from, 
                                """
                                select column_name
                                from INFORMATION_SCHEMA.COLUMNS
                                where TABLE_NAME=:table and TABLE_SCHEMA =:db
                                """, 
                                db=db_from, 
                                table=table_from, 
                                output='df')['column_name'].tolist()
            
    if column_matching_method == 'both':
        columns_bring = list(set(columns_df_from).intersection(set(columns_df_to)))
    elif column_matching_method == 'to':
        columns_bring = columns_df_to
    elif column_matching_method == 'from':
        columns_bring = columns_df_from
    else:
        raise Exception('not proper matching_mode')
    return columns_bring

