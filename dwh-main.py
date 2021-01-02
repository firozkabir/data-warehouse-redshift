#!/usr/bin/env python3

import sys
from sql_queries import *
from datetime import datetime
import psycopg2


def get_config(config_file='dwh.cfg'):
    
    try:
        
        config = configparser.ConfigParser()
        config.read_file(open(config_file))
        return config
        
    except Exception as e:
        print(f"ERROR while trying to read config file dwh.cfg: {e}")
        return None


def show_sql(sql_dict):
    

    for key in sql_dict:
        sql = sql_dict[key]
        print(f"Showing you sql text for {key}:")
        print(f"{sql}")


def run_sql(sql_dict):
    
    
    try:
        config = get_config()
        host = config['CLUSTER']['HOST']
        db_name = config['CLUSTER']['DB_NAME']
        db_user = config['CLUSTER']['DB_USER']
        db_password = config['CLUSTER']['DB_PASSWORD']
        db_port = config['CLUSTER']['DB_PORT']
        connection = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=host, port=db_port)
        cursor = connection.cursor()
        
        for key in sql_dict:
            sql = sql_dict[key]
            print(f"Working on {key}")
            cursor.execute(sql)
            connection.commit()

    except Exception as e:
        print(f"ERROR: {e}")
        raise e
    
    finally:
        if connection is not None:
            connection.close()


def create_schema():

    config = get_config()
    if not config:
        print(f"Config object is empty. Exiting program.")
        sys.exit(1)

    run_sql(drop_table_queries)
    run_sql(create_table_queries)


def do_etl():

    config = get_config()
    if not config:
        print(f"Config object is empty. Exiting program.")
        sys.exit(1)

    run_sql(copy_table_queries)
    run_sql(insert_table_queries)


def main(argv):
    print(f"*** start - {datetime.now()} ***")


    user_command = argv[1]

    if user_command == 'create_schema':
        create_schema()
    
    elif user_command == 'do_etl':
        do_etl()
    
    else:
        pass

    


    print(f"=== end - {datetime.now()} ===")


if __name__ == '__main__':
    main(sys.argv)