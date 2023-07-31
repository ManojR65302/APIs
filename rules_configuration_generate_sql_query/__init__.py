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

    if action == 'fetch':
        sql = """select sql_statement, json_output from rules_config_sql_query_table"""
        cursor.execute(sql)
        rows = cursor.fetchall()

        objects_list = []

        for row in rows:
            d = collections.OrderedDict()
            d["SQL_Query"] = row[0]
            d["Json_output"] = row[1] 

            objects_list.append(d)

        cursor.close()
        conn.close()
        return func.HttpResponse(json.dumps(objects_list), status_code=200)

    
    if action == 'insert':
        req_body = req.get_body()
        data = json.loads(req_body)

        rule_id = data['Rule_ID']
        rule_name = data['Rule_Name']
        populate_field = data['Populate_Field']
        populate_value = data['Populate_Value']   
        logical_condition = data['Logical_Condition']
        json_output = data['Json_Output']
        #Convert the dictionary to a string
        json_output= json.dumps(json_output)

        query = f"UPDATE regulation_requirement_central_repository A SET "
        query += f"{populate_field} = IF((isnull({populate_field}) OR {populate_field} = ''), '{populate_value}', "
        query += f"IF({populate_field} LIKE '%{populate_value}%', {populate_field}, CONCAT({populate_field}, ', ', '{populate_value}'))), "
        query += f"Rule_ID = IF((isnull(Rule_ID) OR Rule_ID = ''), '{rule_id}', "
        query += f"IF(Rule_ID LIKE '%{rule_id}%', Rule_ID, CONCAT(Rule_ID, ', ', '{rule_id}')))"
        for condition in logical_condition:
            l_condition1 = condition["logical_condition1"]
            check_field = condition["field"]
            l_condition2 = condition["logical_condition2"]
            check_value = condition["value"]
            query += f" {l_condition1} {check_field} {l_condition2} '%{check_value}%'"
        query += f";"
        

        mySql_insert_query = """INSERT INTO rules_config_sql_query_table (document_id, rule_id, sql_statement, json_output) VALUES( (select document_id from rules_list where rule_name = %s), %s,%s,%s) """
        record = (rule_name, rule_id, query, json_output)
        cursor.execute(mySql_insert_query, record)
        conn.commit()

        if cursor.rowcount != 0:
            result = {'Status' : 200 , 'Message' :'Records Inserted' }
        else:
            result = {'Status' : 100 , 'Message' :'Records Not Intserted' }

        # cursor.execute(query)
        # conn.commit()

        # if cursor.rowcount != 0:
        #     result1 = {'Status' : 200 , 'Message' :'Records Updated' }
        # else:
        #     result1 = {'Status' : 100 , 'Message' :'Records Not Updated' }

        # Cleanup

        cursor.close()
        conn.close()
        print("Done.")

        return func.HttpResponse(f'{result} ', status_code=200)  #- {result1}


    if action == 'delete':

            req_body = req.get_body()
            data = json.loads(req_body)
    
            rule_id = data['Rule_ID']        
            
            for i in rule_id:
                delete = """ delete from rules_config_sql_query_table where rule_id = %s """
                record = (i,)
                cursor.execute(delete, (record))
                conn.commit()

            if cursor.rowcount != 0:
                result = {'Status' : 200 , 'Message' :'Records Deleted' }
            else:
                result = {'Status' : 100 , 'Message' :'Not Deleted' }

            return func.HttpResponse(json.dumps(result), status_code=200)
    
    if action == 'edit':

        req_body = req.get_body()
        data = json.loads(req_body)

        rule_id = data['Rule_ID']
        populate_field = data['Populate_Field']
        populate_value = data['Populate_Value']   
        logical_condition = data['Logical_Condition']
        json_output = data['Json_Output']
        #Convert the dictionary to a string
        json_output= json.dumps(json_output)

        query = f"UPDATE regulation_requirement_central_repository A SET "
        query += f"{populate_field} = IF((isnull({populate_field}) OR {populate_field} = ''), '{populate_value}', "
        query += f"IF({populate_field} LIKE '%{populate_value}%', {populate_field}, CONCAT({populate_field}, ', ', '{populate_value}'))), "
        query += f"Rule_ID = IF((isnull(Rule_ID) OR Rule_ID = ''), '{rule_id}', "
        query += f"IF(Rule_ID LIKE '%{rule_id}%', Rule_ID, CONCAT(Rule_ID, ', ', '{rule_id}')))"
        for condition in logical_condition:
            l_condition1 = condition["logical_condition1"]
            check_field = condition["field"]
            l_condition2 = condition["logical_condition2"]
            check_value = condition["value"]
            query += f" {l_condition1} {check_field} {l_condition2} '%{check_value}%'"
        query += f";"

        sql_query = """UPDATE rules_config_sql_query_table SET sql_statement = %s, json_output = %s 
                                WHERE rule_id = %s"""
        cursor.execute(sql_query, (query, json_output,
                       rule_id,))
        conn.commit()

        if cursor.rowcount != 0:
            result = {"statuscode": 200, "message": "Records Updated"}
        else:
            result = {"statuscode": 200, "message": "Records Not Updated"}

        # Cleanup
        cursor.close()
        conn.close()
 
        return func.HttpResponse(json.dumps(result), status_code=200)
    

       
#     {
#     "Rule_ID" : "M(1)",
#     "Rule_Name" : "KJG",
#     "Populate_Field" : "obligations_to",
#     "Populate_Value" : "Manufacturer",
#     "Logical_Condition" : [
#         {
#            "logical_condition" : "AND",
#            "field" : "value2",
#            "value" : "value3"
#            },
#            {
#              "logical_condition" : "OR",
#              "field" : "value2",
#              "value" : "value3"
#            }
#      ],
#     "Json_Output" : [
#         {
#             "Rule_ID" : "M(1)",
#             "Document_ID" : "4",
#             "Populate_Field" : "obligations_to",
#             "Populate_Value" : "Manufacturer",
#             "Logical_Condition" : "WHERE",
#             "Check_Field" : "requirement_text",
#             "Check_Value" : "responsibility of manufacturers"
#         }
#     ]
# }