import pandas as pd
import numpy as np 

from ..pyjin import pyjin
from . import db_module

def get_df_whole(acc, 
                 db, 
                 table, 
                 columns):
    with pyjin.connectDB(**acc, engine_type='NullPool') as con:         
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")            
        df=pyjin.execute_query(con,
                                """
                                select {columns} from {db}.{table}
                                """.format(columns=','.join(columns), db=db, table=table)
                                , output='df')        
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    return df

def get_insert_update_delete_ids(df_from, df_to, primary_key, update_date_col, mode):        
    ids_from = df_from[primary_key].to_numpy()
    ids_to = df_to[primary_key].to_numpy()

    ## delete ids
    delete_ids = np.setdiff1d(ids_to, ids_from)

    if mode == 'id_update_date':
        # update 할 id
        temp = pd.merge(df_from, df_to, on=primary_key)
        # 타입이 다른 경우 string으로 통일
        if temp['{}_x'.format(update_date_col)].dtypes != temp['{}_y'.format(update_date_col)].dtypes:
            temp['{}_x'.format(update_date_col)] = temp.apply(lambda x: str(x['{}_x'.format(update_date_col)]), axis=1)
            temp['{}_y'.format(update_date_col)] = temp.apply(lambda x: str(x['{}_y'.format(update_date_col)]), axis=1) 
        update_ids = temp[temp['{}_x'.format(update_date_col)] != temp['{}_y'.format(update_date_col)]][primary_key].to_numpy()        
    else: # update 필드가 없는경우
        update_ids = np.array([])
        
    insert_ids = np.setdiff1d(ids_from, ids_to)
    return insert_ids, update_ids, delete_ids


def get_upsert_data(acc_from, 
                    db_from, 
                    table_from, 
                    primary_key, 
                    list_insert_update_ids,
                    sync_columns):
    
    with pyjin.connectDB(**acc_from, engine_type='NullPool') as con:
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")            
        df_upsert = pyjin.execute_query(con,
                                        """
                                        select {columns} from {db}.{table} where {primary_key} in :ids
                                        """.format(columns= '`'+'`,`'.join(sync_columns)+'`',
                                            db= db_from,
                                            table=table_from,
                                            primary_key=primary_key), 
                                        ids=list_insert_update_ids,
                                        output='df')                                 
        pyjin.execute_query(con,"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")            
    
    return df_upsert  


def delete_upsert(acc_to,
                  db_to,
                  table_to,
                  update_ids,
                  insert_ids,
                  delete_ids,
                  list_insert_update_ids,
                  list_delete_update_ids,
                  primary_key,
                  df_upsert=None,):    
    
    ## delete , upsert
    with pyjin.connectDB(**acc_to, engine_type='NullPool') as con:         
        with con.begin():
            # delete_id, update_id 삭제하기
            if len(list_delete_update_ids):
                ## delete upsert_ids
                pyjin.execute_query(con,"SET foreign_key_checks = 0")
                ## release foreignkey when deleting table and restore
                pyjin.execute_query(con, 
                                        """
                                        delete from {}.{} where {} in :ids
                                        """.format(db_to, table_to, primary_key),
                                        ids = list_delete_update_ids,                                          
                                        is_return=False)
                pyjin.execute_query(con,"SET foreign_key_checks = 1")
    
            pyjin.print_logging('{} (update), {} (delete) rows deleted'.format(len(update_ids), len(delete_ids)))
            
            if df_upsert is not None:
                # update_id, insert_id 정보 업데이트 하기
                if len(list_insert_update_ids):
                    pyjin.execute_query(con,"SET foreign_key_checks = 0")
                    df_upsert.to_sql(table_to, con=con, schema=db_to, index=False, if_exists='append', chunksize=5000, method='multi')
                    pyjin.execute_query(con,"SET foreign_key_checks = 1")

            pyjin.print_logging('{} (update), {} (insert) data inserted'.format(len(update_ids), len(insert_ids)))
    
    return True


class Main:            
    def __init__(self, 
                 acc_to,
                 acc_from,
                 db_to,
                 db_from,
                 table_to,
                 table_from,
                 primary_key,                 
                 update_date_col,
                 mode,
                 column_matching_method = 'to',
                 **kwargs):
        
        self.acc_to = acc_to
        self.acc_from = acc_from
        self.db_to = db_to
        self.db_from = db_from
        self.table_to = table_to
        self.table_from = table_from
        self.primary_key = primary_key
        self.column_matching_method = column_matching_method
        self.update_date_col = update_date_col
        self.mode = mode

        
    def __call__(self,):
        self.main() 
        return True       
    
    def set_df_to(self, columns):
        res = get_df_whole(db= self.db_to,
                           acc = self.acc_to,
                           table = self.table_to,
                           columns= columns)     
        return res
        
    def set_df_from(self, columns):
        res = get_df_whole(db= self.db_from,
                           acc = self.acc_from,
                           table = self.table_from,
                           columns= columns)   
        return res
        
    def set_upsert_data(self, list_insert_update_ids, sync_columns): 
        df_upsert = get_upsert_data(acc_from = self.acc_from,                                                                                  
                                    db_from = self.db_from,
                                    table_from = self.table_from,
                                    primary_key = self.primary_key,
                                    list_insert_update_ids= list_insert_update_ids,
                                    sync_columns=sync_columns)
        return df_upsert
        
    def set_column_matching(self,):
        res= db_module.get_col_matched(acc_from = self.acc_from,
                                        acc_to = self.acc_to, 
                                        db_from = self.db_from, 
                                        db_to = self.db_to, 
                                        table_from = self.table_from, 
                                        table_to = self.table_to, 
                                        column_matching_method = self.column_matching_method)
        return res
        
            
    def main(self,):                

        upsert_base_columns = []
        if self.primary_key:
            upsert_base_columns.append(self.primary_key)
        if self.update_date_col:
            upsert_base_columns.append(self.update_date_col)
        

        df_to = self.set_df_to(upsert_base_columns)
        df_from = self.set_df_from(upsert_base_columns)
        
        '''
        update 할것과 delete 할것을 -> delete
        update 할것과 insert 할것 -> insert
        (update data는 사실상 replaced)
        '''                        
        ## calculate insert_ids, update_ids, delete_ids
        insert_ids, update_ids, delete_ids = get_insert_update_delete_ids(df_from = df_from,
                                                                          df_to = df_to,
                                                                          primary_key = self.primary_key,
                                                                          update_date_col = self.update_date_col,
                                                                          mode= self.mode)    
    
        
        list_delete_update_ids = delete_ids.tolist() + update_ids.tolist()
        list_insert_update_ids = insert_ids.tolist() + update_ids.tolist()
        
        # update ids 있는경우
        if len(list_insert_update_ids):             
            sync_columns = self.set_column_matching()
            df_upsert = self.set_upsert_data(
                                    list_insert_update_ids= list_insert_update_ids,
                                    sync_columns = sync_columns)
        else:
            df_upsert = None               
            
            
        try:        
            delete_upsert(df_upsert = df_upsert,
                        acc_to = self.acc_to,
                        db_to = self.db_to,
                        table_to = self.table_to,
                        delete_ids = delete_ids,
                        update_ids = update_ids,
                        insert_ids = insert_ids,
                        list_insert_update_ids = list_insert_update_ids,
                        list_delete_update_ids = list_delete_update_ids,
                        primary_key = self.primary_key)            
                       
        except BaseException as e:        
            raise Exception(e)                        
        


def get_col_matched_from_df(df_from,
                    acc_to,                     
                    db_to,                     
                    table_to, 
                    column_matching_method):
    
    ## bring all columns of the table_to
    columns_to = pyjin.conn_exec_close(acc_to, 
                                    """
                                    select column_name
                                    from INFORMATION_SCHEMA.COLUMNS
                                    where TABLE_NAME=:table and TABLE_SCHEMA =:db
                                    """, 
                                    db=db_to, 
                                    table=table_to, 
                                    output='df')['column_name'].tolist()   
    
    columns_from = df_from.keys()
    
            
    if column_matching_method == 'both':
        columns_bring = list(set(columns_from).intersection(set(columns_to)))
    elif column_matching_method == 'to':
        columns_bring = columns_to
    elif column_matching_method == 'from':
        columns_bring = columns_from
    else:
        raise Exception('not proper matching_mode')
    return columns_bring


class Main_by_json(Main):     
     
    def __init__(self, 
                 acc_to,
                 df_from,
                 db_to,                 
                 table_to,
                 primary_key,
                 mode,                
                 column_matching_method):
        
        self.acc_to = acc_to
        self.db_to = db_to                
        self.table_to = table_to
        self.primary_key = primary_key
        self.column_matching_method = column_matching_method
        self.mode = mode
        self.df_from = df_from

    def set_df_from(self, 
                    update_mode):
        
        return self.df_from        

    def get_upsert_data(self, sync_columns):
        df_upsert = self.df_from
        return df_upsert

    def set_column_matching(self,):
        columns_bring= get_col_matched_from_df(input_dict = self.df_from,
                                                acc_to= self.acc_to,
                                                db_to = self.db_to,
                                                table_to = self.table_to,
                                                column_matching_method = self.column_matching_method)
                
        return columns_bring
            
