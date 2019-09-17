#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy as sql
import pandas as pd
import csv, sys
from tqdm import tqdm


# handle field limit error.
maxsize = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxsize)
    except:
        maxsize = int(maxsize/10)

tmp_parser = configparser.ConfigParser()
tmp_parser.read('.lightsaildb-creds')
cred_user = tmp_parser.get('default-lightsail', 'USER')
cred_password = tmp_parser.get('default-lightsail', 'PASSWORD')
cred_endpoint = tmp_parser.get('default-lightsail', 'ENDPOINT')
cred_port = int(tmp_parser.get('default-lightsail', 'PSQL_PORT'))


class PushDatabases():
    def __init__(self, cred_user=cred_user, cred_password=cred_password, cred_endpoint=cred_endpoint, cred_port=cred_port, db=None):
        self.user = cred_user
        self.password = cred_password
        self.endpoint = cred_endpoint
        self.port = cred_port
        if db is None:
            self.db = "dbmaster"
        else:
            self.db = db
        self.postgres_conn = psycopg2.connect(host=self.endpoint,
                                              dbname=self.db,
                                              user=self.user,
                                              password=self.password,
                                              port=self.port)


    def __parse_interpolator(self, n, interpolator='%s'):
        tmp_list = [interpolator for _ in range(n)]
        return ", ".join(tmp_list)

    def __list_existing_db(self):
        cursor = self.postgres_conn.cursor()
        cursor.execute("""SELECT datname FROM pg_database""")
        preserve_names = [i[0] for i in cursor.fetchall()]
        cursor.close()
        return preserve_names

    def __check_existing_users(self):
        cursor = self.postgres_conn.cursor()
        cursor.execute("SELECT usename FROM pg_user;")
        preserve_names = [i[0] for i in cursor.fetchall()]
        cursor.close()
        return preserve_names

    def __concat_colname_coltype(self, attribute_dict):
        tmp_string_holder = ""
        for index, (colname, coltype) in enumerate(attribute_dict.items()):
            if index + 1 < len(attribute_dict):
                tmp_string_holder += f"{colname} {coltype}, "
            else:
                tmp_string_holder += f"{colname} {coltype}"
        return tmp_string_holder

    def grant_read_only_permissions_to_database(self, dbname, user):
        cursor = self.postgres_conn.cursor()
        if user in self.__check_existing_users() is False:
            username = input("Create user: ")
            password = input("User password: ")
            cursor.execute(f"CREATE ROLE {username} WITH LOGIN PASSWORD '{password}' NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB NOREPLICATION;")
            print(f"User {username} with password {password} successfully created. Please store this access")
        if user is cred_user:
            raise Warning("Master admin should not be modified")

        self.postgres_conn.commit()
        cursor.close()

        cursor = self.postgres_conn.cursor()
        grant_connect = f"GRANT CONNECT ON DATABASE {dbname} TO {user};"
        print(grant_connect, "\tOK")
        cursor.execute(grant_connect)
        self.postgres_conn.commit()
        grant_usage = f"GRANT USAGE ON SCHEMA public TO {user};"
        cursor.execute(grant_usage)
        print(grant_usage, "\tOK")
        self.postgres_conn.commit()
        grant_select = f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user}"
        cursor.execute(grant_select)
        print(grant_select, "\tOK")
        self.postgres_conn.commit()
        cursor.close()





    def print_existing_db(self):
        for i in self.__list_existing_db():
            print(i)

    def create_database(self,dbname):
        isolated_conn = self.postgres_conn
        isolated_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        isolated_cursor = isolated_conn.cursor()
        order = f"CREATE DATABASE {dbname}"
        isolated_cursor.execute(order)
        isolated_cursor.close()


    def create_table(self, data, dbname, tablename, attribute_dict, header=True, nas={'': 99999}):

        if dbname in self.__list_existing_db() is False:
            raise Warning("Database does not exists")
        if isinstance(attribute_dict, dict) is False:
            raise TypeError("Attribute dictionary must be column:type key pair")

        cursor = self.postgres_conn.cursor()


        columns_coltypes = self.__concat_colname_coltype(attribute_dict)
        cursor.execute(f"CREATE TABLE {tablename} ({columns_coltypes});")
        print(f"Table {tablename} has been successfully created.")
        self.postgres_conn.commit()
        cursor.close()

        cursor = self.postgres_conn.cursor()

        if 'PRIMARY KEY' in attribute_dict.keys():
            remove_key = attribute_dict.pop('PRIMARY KEY')
        if 'FOREIGN KEY' in attribute_dict.keys():
            remove_key = attribute_dict.pop('FOREIGN KEY')
        if 'id' in attribute_dict.keys():
            remove_key = attribute_dict.pop('id')

        with open(data, 'r') as file:
            if file.name.split('.')[-1] == 'tsv':
                reader = csv.reader(file, delimiter='\t')
            else:
                reader = csv.reader(file)
            if header is True:
                next(reader)
            row_length_interpolator = self.__parse_interpolator(len(attribute_dict))
            columns = ', '.join(attribute_dict.keys())

            print(row_length_interpolator)
            for row in tqdm(reader):
                row = [99999 if i == '' else i for i in row]
                cursor.execute(f"INSERT INTO {tablename} ({columns}) VALUES ({row_length_interpolator});", row)
                self.postgres_conn.commit()

        cursor.close()

