import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (Dimension, Metric, DateRange, Filter, FilterExpression,RunReportRequest)
from google.oauth2 import service_account



class GAtobanco():
    def __init__(
        self,
        list_dim: list,
        list_metrics: list,
        task_id: str,
        property_id: int = 5,
        pausa_response: int = 20,
        list_null_columns: list = [],
        tam_req: int = 100000,
        offset: int = 0,
        colunamerge: str = 'id',
        filter_field_name: str = "eventName",
        filter_string_filter: str = "y",
        date_run_start: str = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'),
        date_run_end: str = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d'),
        

        *args, **kwargs
    ):
        super().__init__(task_id = task_id, *args, **kwargs)
        self.list_dim = list_dim
        self.list_metrics = list_metrics
        self.table_name = table_name
        self.pausa_response = pausa_response
        self.list_null_columns = list_null_columns
        self.property_id = property_id
        self.tam_req = tam_req
        self.offset = offset
        self.filter_field_name = filter_field_name
        self.filter_string_filter = filter_string_filter
        self.date_run_start = date_run_start
        self.date_run_end = date_run_end



    def query_report(self, list_dim):
        chave = get("GOOGLE_CREDENTIALS", deserialize_json=True)

        credentials = service_account.Credentials.from_service_account_info(chave)

        client = BetaAnalyticsDataClient(credentials=credentials)
        dimension_list = [Dimension(name=dim) for dim in list_dim]
        metrics_list = [Metric(name=m) for m in self.list_metrics]

        report_request = RunReportRequest(
            property=f'properties/{self.property_id}',
            dimensions=dimension_list,
            metrics=metrics_list,
            limit=self.tam_req,
            offset=self.offset,
            date_ranges=[DateRange(start_date=self.date_run_start, end_date=self.date_run_end)],
            dimension_filter=FilterExpression(filter=Filter(
                field_name=self.filter_field_name,
                string_filter=Filter.StringFilter(value=self.filter_string_filter)
            ))
        )
        
        response = client.run_report(report_request)
        return response
    

    def parse_data(self, response):
        #response = self.query_report()
        # criando dicionários vazios para dimensões e métricas
        dim_data = {header.name: [] for header in response.dimension_headers}
        metric_data = {header.name: [] for header in response.metric_headers}


        # Interando por linha e preencher dimensões e dicionários de métricas
        for i, row in enumerate(response.rows):
            for j, dim in enumerate(row.dimension_values):
                dim_data[response.dimension_headers[j].name].append(value)
            for k, metric in enumerate(row.metric_values):
                metric_data[response.metric_headers[k].name].append(metric.value)

        # Criando dataframes a partir dos dicionários
        dim_df = pd.DataFrame(dim_data)

        return dim_df


    
    def execute(self, context):
        self.date_run_end = context.get("data_interval_start").strftime('%Y-%m-%d')
        self.date_run_start = (datetime.strptime(self.date_run_end, '%Y-%m-%d') - timedelta(1)).strftime('%Y-%m-%d')
        if datetime.strptime(self.date_run_end, '%Y-%m-%d').weekday()==4:
            self.date_run_end = (datetime.strptime(self.date_run_start, '%Y-%m-%d') + timedelta(3)).strftime('%Y-%m-%d')
            
        print(f"Executando data: {self.date_run_start} ate {self.date_run_end}")
        #deleta historico para reprocessar
        self.delete_history(context)
        
        df = pd.DataFrame()
        
        # Aqui retorna um df completo
        df = self.parse_data()

        if df.empty:
            
            print("Empty dataframe")
            
            return False
        else:

            total_linhas = len(df)
            print("Completed dataframe")
            return True