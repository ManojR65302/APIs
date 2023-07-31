import logging
import json
import collections
import azure.functions as func
import mysql.connector
from mysql.connector import errorcode


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    config = {
        'host': 'aeroregulatoriesdb.mysql.database.azure.com',
        'user': 'aerodbadmin',
        'password': 'Internet@123',
        'database': 'aerodb'
    }

    try:
        conn = mysql.connector.connect(**config)
        print("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = conn.cursor()

    sql = """SELECT DISTINCT country_name FROM cyarcapp_country order by country_name;"""
    cursor.execute(sql)
    rows = cursor.fetchall()

    objects_list = []
    for row in rows:
        d = collections.OrderedDict()
        d['country_name'] = row[0]
        objects_list.append(d)

    cursor.close()
    conn.close()

    return func.HttpResponse(json.dumps(objects_list), status_code=200)
