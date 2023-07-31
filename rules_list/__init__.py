import logging
import json
from urllib.parse import parse_qs
import azure.functions as func
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from datetime import date
import urllib.request
import collections


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    action = req.params.get('action')

    config = {
        'host': 'medicalregulatoryplatformdb.mysql.database.azure.com',
        'user': 'mrpdbadmin@medicalregulatoryplatformdb',
        'password': 'dbadmin@12345',
        'database': 'medical_regulatory_repository'
    }
    
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    if action == 'fetch_rules':
        sql = """select rule_name, document_name, country, id from document_wise_source_details d, rules_list r where d.sno = r.document_id"""
        cursor.execute(sql)
        rows = cursor.fetchall()

        objects_list = []

        for row in rows:
            d = collections.OrderedDict()
            d["Rule_Name"] = row[0]
            d["Document_Name"] = row[1]
            d["Country"] = row[2]
            d["id"] = row[3]

            objects_list.append(d)

        cursor.close()
        conn.close()
        return func.HttpResponse(json.dumps(objects_list), status_code=200)

    if action == 'insert':
        req_body = req.get_body()
        data = json.loads(req_body)

        rule_name = data['rule_name']
        country_name = data['country_name']
        document_name = data['document_name']

        mySql_insert_query = """insert into rules_list ( document_id, rule_name) Values ((select sno from document_wise_source_details where document_name = %s and country = %s) , %s)"""
        record = (document_name, country_name, rule_name)
        cursor.execute(mySql_insert_query, record)
        conn.commit()

        if cursor.rowcount != 0:
            result = {'Status' : 200 , 'Message' :'Records Inserted' }
        else:
            result = {'Status' : 100 , 'Message' :'Records Not Intserted' }

        cursor.close()
        conn.close()
        return func.HttpResponse(result, status_code=200)

    if action == 'delete_rules':
        req_body = req.get_body()
        data = json.loads(req_body)
        
        rule_name = data['rule_name']
        country_name = data['country_name']
        document_name = data['document_name']

        sql = """delete rules_list from rules_list left join document_wise_source_details on rules_list.document_id = document_wise_source_details.sno where document_wise_source_details.document_name = %s and document_wise_source_details.country =%s  and rules_list.rule_name= %s"""
        record = (document_name, country_name, rule_name)
        cursor.execute(sql, record)
        conn.commit()

        if cursor.rowcount != 0:
            result = {'Status': 200, 'Message': 'Records Deleted'}
        else:
            result = {'Status': 100, 'Message': 'Not Deleted'}

        cursor.close()
        conn.close()
        return func.HttpResponse(json.dumps(result), status_code=200)

    if action == 'edit':

        req_body = req.get_body()
        data = json.loads(req_body)

        rule_name = data['rule_name']
        id = data['id']
        document_name = data['document_name']
        country_name = data['country_name']

        sql_query = """UPDATE rules_list SET rule_name = %s, document_id = (select sno from document_wise_source_details where document_name = %s and country = %s) WHERE id = %s;"""
        cursor.execute(sql_query, (rule_name, document_name, country_name, id))
        conn.commit()

        if cursor.rowcount != 0:
            result = {"statuscode": 200, "message": "Records Updated"}
        else:
            result = {"statuscode": 200, "message": "Records Not Updated"}

        # Cleanup
        cursor.close()
        conn.close()
        return func.HttpResponse(json.dumps(result), status_code=200)    

    
    # "rule_name" : "IVDR23",
    # "country_name" : "European Union",
    # "document_name" : "Council Directive 93/42/EEC on Medical Devices (MDD)",
    # "id" : 18

