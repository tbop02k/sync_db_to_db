import sys

from . import account_info as ai
from . import core

class Infor:        
    table_list = []        
    
    table_list.append({
        'acc_from' : ai.servicedb_sql_info(),
        'acc_to' : ai.analdb_sql_info(),
        'db_from': 'ople',
        'table_from' : 'rest_product', # table name you want to syncronize from
        'db_to' : 'ople', # schema name you want to syncronize to
        'table_to' : 'rest_product', # table name you want to syncronize to
        'primary_key' : 'id',
        'update_date_col' : 'update_date' # datetime field for update criterion
    })
            

def main():  
    
    for dict_infor in Infor.table_list:        
        core.main(**dict_infor)
