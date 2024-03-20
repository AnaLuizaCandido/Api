import sys
import json
import requests
from pandas import json_normalize
import pandas as pd
from dotenv import load_dotenv
import sqlalchemy as sa
from datetime import datetime as DT
import psycopg2

class zohoConnect():
    def __init__(
        self,
        task_id: str,
        table_name: str,
        path: str,
        redshift_conn_id:str,
        replace_dtypes:dict ={
        "id" :"int"
        , "ticketNumber" :"int"
        },
        *args, **kwargs
    ):
        super().__init__(task_id = task_id, *args, **kwargs)
        self.task_id= task_id
        self.task_id= task_id
        self.path = path
        self.replace_dtypes=replace_dtypes

     
     
     
    def get_token(self,context):
                client_secret = Variable.get("client_secret", deserialize_json=False)
                zoho_token = Variable.get("zoho_desk_token", deserialize_json=False)
                
                url = "https://accounts.zoho.com/oauth/v2/token"

                payload = f'refresh_token={zoho_token}&client_id={client_id}&client_secret={client_secret}&grant_type=refresh_token'
                headers = {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        }

                response = requests.request("POST", url, headers=headers, data=payload)

                resposta_token = response.text
                token  = resposta_token[17:87]   
                
                return token
        
    def get_data(self, context):
        


                #Pag
                more_records = True
                page_number = 0
                from1=0
                token=self.get_token(context)
                                
                cases_table = pd.DataFrame()

                while more_records:

                        url = f"""https://desk.zoho.com/api/v1/tickets?from={from1}&limit=100&departmentIds=7&fields=ticketNumber,createdTime,priority&include=assignee,team"""
                        
                        payload = ""
                        
                        headers = {'Authorization':f'Zoho-oauthtoken {token}', 
                                   'Content-Type':'application/json'    
                        }
                        
                        r = requests.request("GET", url, headers=headers, data=payload)
                        
                        #ajust response
                        table = r.text
                        
                        # Add columns to df
                        info = json.loads(table)
                        table = json_normalize(info["data"])
                        
                        #concat
                        cases_table = pd.concat([table, cases_table])

                        #pagination
                        page_number += 1
                        from1 = (page_number * 100) +1 
                        
                        print("quantidade de registros: ",table["id"].count())
                        print("Total até agora: ", cases_table["id"].count())
                        if (table["id"].count()==100):
                            more_records = True
                        else:
                            more_records = False

                
                ids_list = cases_table["id"].tolist()
                
                
                print(cases_table.columns)

                
                 # first response column
                final_cases_table = self.obter_metricas(cases_table,ids_list,context)
    
                return final_cases_table
               
            
    def obter_metricas(self, cases_table,ids_list,context):
        
        token=self.get_token(context)
        headers = {'Authorization':f'Zoho-oauthtoken {token}', 
                                   'Content-Type':'application/json'    
                        }



        # Substitua com suas credenciais e IDs de ticket
        ticket_ids = ids_list
        # Criar um DataFrame vazio
        df = pd.DataFrame(columns=["id", "firstResponseTime"])

        for ticket_id in ticket_ids:
            print("ticket_number", ticket_id)
            url = f"https://desk.zoho.com/api/v1/tickets/{ticket_id}/metrics"
            
            metrics = requests.get(url, headers=headers)
            info = json.loads(metrics.text)
            
            #edit colum format
            
            parts = info["firstResponseTime"].split(':')
            hours = int(parts[0])
            minutes = int(parts[1].split()[0])
            time=hours + minutes / 60

            # Append new data
            df = df.append({"id": ticket_id, "firstResponseTime": time}, ignore_index=True)
             
        #merge cases_table with firstResponseTime
        cases_table_final = pd.merge(cases_table, df, on="id") 
        
     
        return cases_table_final 

    
    def execute(self, context):
        df_final = self.get_data(context)
        print(df_final.dtypes)
        
       
    