
import json
import requests
from pandas import json_normalize
import pandas as pd
import sqlalchemy as sa
from datetime import datetime as DT
import psycopg2

class ZohoCRM():
    def __init__(
        self,
        redshift_conn_id:str,
        zoho_token_user:str,
        fields:str,
        replace_dtypes:dict ={
        "id" :"int"
        , "ticketNumber" :"int"
        },
        *args, **kwargs
    ):
        super().__init__(task_id = task_id, *args, **kwargs)
        self.task_id= task_id
        self.table_name= table_name
        self.task_id= task_id
        self.schema= schema
        self.aws_bucket_name= aws_bucket_name
        self.s3_key=s3_key
        self.aws_conn_id= aws_conn_id
        self.path = path
        self.redshift_conn_id=redshift_conn_id
        self.replace_dtypes=replace_dtypes
        self.zoho_token_user = zoho_token_user
        self.fields = fields

        
        
    def get_data(self, context):
        
                client_secret = Variable.get("client_secret")
                
                #Generate token
                url = "https://accounts.zoho.com/oauth/v2/token"

                payload = f'refresh_token={self.zoho_token_user}&client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token'
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Cookie': ''
                }

                response = requests.request(
                    "POST", url, headers=headers, data=payload)

                resposta_token = response.text
                last_token = resposta_token[17:87]

                #case
                more_records = True
                page_number = 10
                offset = 0
                cases_table = pd.DataFrame()
                
                url = f"https://www.zohoapis.com/crm/v6/Cases?fields={self.fields}&page=1"
                payload = "\n                        "
                headers = {
                    'Authorization': f'Zoho-oauthtoken {last_token}',
                    'Content-Type': 'text/plain',
                   }

                response = requests.request("GET", url, headers=headers, data=payload)

                    #ajust response
                table = response.text

                # Add columns to df
                info = json.loads(table) 
                
                table = json_normalize(info["data"])

                #concat
                cases_table = pd.concat([table, cases_table])

                records = json_normalize(info["info"])
                page_token = records.iat[0,1]
                

                while more_records:
                        url = f"https://www.zohoapis.com/crm/v6/Cases?fields={self.fields}&page_token={page_token}"

                        response = requests.request("GET", url, headers=headers, data=payload)

                        # Ajust response
                        table = response.text

                        # Add columns to df
                        info = json.loads(table)  
                        table = json_normalize(info["data"])

                        # Concat
                        cases_table = pd.concat([table, cases_table])

                        # Pagination
                        page_number += 1
                        records = json_normalize(info["info"])
                        print("Page number: ",records.iat[0,5])
                        more_records = records.iat[0,8]
                        page_token = records.iat[0,1]
                        
                return cases_table
   
    
    
    def execute(self, context):
        df_final = self.get_data(context)
        print(df_final.dtypes)
        
        
        


    