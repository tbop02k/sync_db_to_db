# Sync DB to DB



## **Introduction**

This module synchronizes data between two databases. 



## Database support

MariaDB, Oracle, Mysql



## How to use

#### account_info.py

write your database accounts you want to syncronize



#### core.py

you can conduct synchronize by passing account information and synchronization settings

```python
{
        'acc_from' : ai.a_sql_info(), # account information you want to synchronize from
        'acc_to' : ai.b_sql_info(),
        'db_from': 'db', # schema name you want to synchronize from
        'table_from' : 'table_name', # table name you want to synchronize from
        'db_to' : 'db', # schema name you want to synchronize to
        'table_to' : 'rest_supplier', # table name you want to synchronize to
        'primary_key' : 'id', # primary key name of both from_table and to_table
        'update_date_col' : 'update_date' # datetime field for update criterion
    }
```



#### sync_db_to_db_multi.py

write your jobs with settings for multiple synchronization.
