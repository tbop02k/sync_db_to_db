import sys

from . import account_info as ai
from . import core

class Infor:        
    table_list = []        
    
    table_list.append({
        'acc_from' : ai.a_sql_info(),
        'acc_to' : ai.b_sql_info(),
        'db_from': 'db', # schema name you want to syncronize from
        'table_from' : 'table_name', # table name you want to syncronize from
        'db_to' : 'db', # schema name you want to syncronize to
        'table_to' : 'rest_supplier', # table name you want to syncronize to
        'primary_key' : 'table_name',
        'update_date_col' : 'update_date' # datetime field for update criterion
    })
            

def main():  
    
    for dict_infor in Infor.table_list:        
        core.main(**dict_infor)