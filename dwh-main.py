#!/usr/bin/env python3

import sys
from sql_queries import *
from datetime import datetime
import psycopg2


def get_config(config_file='dwh.cfg'):
    """
        A function to get config object from file.
        Returns None if it fails to retrieve the data from the file.
    """

    try:
        
        config = configparser.ConfigParser()
        config.read_file(open(config_file))
        return config
        
    except Exception as e:
        print(f"ERROR while trying to read config file dwh.cfg: {e}")
        return None


def show_sql(sql_dict):
    """
        A helper function used to show (print) the sql being executed. 
        Very handing during development / debugging.
    """

    for key in sql_dict:
        sql = sql_dict[key]
        print(f"Showing you sql text for {key}:")
        print(f"{sql}")


def run_sql(sql_dict):
    """
        Gets database credentails, starts a psycopg client. 
        Then iterates over a dictionary of sql statements, to execute each sql in the database
        Commits at the end of the execution. 
    """
    
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
    """
        This function is called in main() to create the star schema in the database
        It will try to drop existing tables before trying to re-create them.
    """
    config = get_config()
    if not config:
        print(f"Config object is empty. Exiting program.")
        sys.exit(1)

    run_sql(drop_table_queries)
    run_sql(create_table_queries)


def do_etl():
    """
        This function is called in main() to perform the ETL tasks
    """
    config = get_config()
    if not config:
        print(f"Config object is empty. Exiting program.")
        sys.exit(1)

    run_sql(copy_table_queries)
    run_sql(insert_table_queries)

def print_usage():
    """
        A helper function to print "usage" text to stdout
    """
    print("Usage: run-dwh.sh <create_schema|do_etl>")
    print("create_schema: drops table if exist before creating them")
    print("do_etl: loads data into staging tables, then loads fact and dimension tables from staging tables.")
    print("Be sure to add all the necessary values in dwh.cfg")



def main(argv):
    """
        As the name suggests, execution starts here
        Uses sys.argv[1] to determine which "user_command" to execute.
    """
    print(f"*** start - {datetime.now()} ***")

    try:
        user_command = argv[1]
    except Exception as e:
        print("At least one argument is required.")
        user_command = ''

    if user_command == 'create_schema':
        create_schema()
    
    elif user_command == 'do_etl':
        do_etl()
    
    else:
        print_usage()
    


    print(f"=== end - {datetime.now()} ===")


if __name__ == '__main__':
    main(sys.argv)