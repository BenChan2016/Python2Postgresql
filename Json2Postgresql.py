# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import psycopg2
import psycopg2.extras
import json


try:
    connector = psycopg2.connect("dbname='rxp' user='postgres' host='localhost' password='admin'")
    with open('/home/hduser/Downloads/generated.json') as json_file:
        data = json.load(json_file);
    try:
        # Define query to be inserted
        SQL_INSERT_QUERY = "INSERT INTO json_table(id, data) VALUES (%s, %s::json)"
        id = 0 ;
        cur = connector.cursor()
        for d in data:
            values = (id, psycopg2.extras.Json(data[id]))
            cur.execute(SQL_INSERT_QUERY, values)
            id = id + 1 ;
        connector.commit()
        cur.close()    
    except:
        print("cannot insert json data to postgresql");
        
except:
    print("cannot connect to postgresql");


