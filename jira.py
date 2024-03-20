from jira import JIRA
import pandas as pd
from datetime import datetime
import time
import requests
import sqlalchemy as sa
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import DateTime as sdate


class jira:
    def getjira(
        jira_token,
    ):
        inicio = time.time()
        print(inicio)
        options = {"server": "https://tecnologia.atlassian.net/"}

        jira = JIRA(options, basic_auth=("email", jira_token))

        # Lista de projetos para recuperar informações

        projetos = [
            ("GNN", "Testes"),
            ("TST", "Gestão"),
            
        ]

        # Lista de campos específicos a serem recuperados das issues
        campos = [
            ("priority", "Prioridade", "prioridade"),
            ("labels", "Categorias labels", "categorias_labels"),
            ("issuelinks", "Itens associados ", "itens_associados"),
            ("assignee", "Responsável", "responsavel"),
            ("status", "Status", "status")

        ]

        # Cria um dicionário vazio para armazenar as informações das issues
        issues_data = {"projeto": [], "projeto_key": [], "issue_id": []}
        issues_data1 = {"projeto": [], "projeto_key": [], "issue_id": []}
        for campo in campos:
            issues_data[campo[2]] = []

        status_options = [
            ("10000", "backlog"),
            ("10861", "revisado"),
            ("10801", "planejado_p_sprint"),
            ("1", "selecionar_p_dev"),
            ("3", "em_andamento_3"),
            ("10101", "em_homologacao"),
            ("10002", "concluido")
        ]
        for st in status_options:
            issues_data1[st[1]] = []

        soma = 0
        c = 0
        # Loop pelos projetos para coletar as informações das issues
        for projeto in projetos:
            issues = jira.search_issues(
                f"project='{projeto[0]}'", maxResults=False, expand="changelog"
            )
            total = issues.total
            print(projeto[0])
            print("total proj: ", total)
            soma += c
            for issue in issues:
                diaissue = datetime.strptime(
                    getattr(issue.fields, "created"), "%Y-%m-%dT%H:%M:%S.%f%z"
                )
                primeirodia = datetime.strptime(
                    "2022-04-01T00:00:00.000-0300", "%Y-%m-%dT%H:%M:%S.%f%z"
                )
                if diaissue > primeirodia:
                    issues_data["projeto_key"].append(projeto[0])
                    issues_data["issue_id"].append(issue.key)
                    issues_data1["projeto"].append(projeto[1])
                    issues_data1["projeto_key"].append(projeto[0])
                    issues_data1["issue_id"].append(issue.key)
                    c = c + 1
                    for campo in campos:
                        issues_data[campo[2]].append(getattr(issue.fields, campo[0]))
                    for history in issue.changelog.histories:
                        for item in history.items:
                            pos_st = None
                            if item.field == "status":
                                for st in status_options:
                                    if st[0] == item.to:
                                        pos_st = st[1]
                                if pos_st is not None:
                                    if len(issues_data1[pos_st]) < len(
                                        issues_data1["projeto_key"]
                                    ):
                                        issues_data1[pos_st].append(
                                            datetime.strptime(
                                                history.created,
                                                "%Y-%m-%dT%H:%M:%S.%f%z",
                                            )
                                        )

                                pos_st = None

                    lengths = [len(v) for v in issues_data1.values()]
                    max_length = max(lengths)
                    for key, value in issues_data1.items():
                        if len(value) < max_length:
                            issues_data1[key].extend(
                                [float("nan")] * (max_length - len(value))
                            )

            print("issues adicionadas: ", c)

        fim = time.time()
        print(fim - inicio)

        # Cria um dataframe pandas com as informações das issues
        lengths = [len(v) for v in issues_data1.values()]
        print(lengths)

        lengths2 = [len(v) for v in issues_data.values()]
        print(lengths2)

        max_length = max(lengths)
        for key, value in issues_data.items():
            if len(value) < max_length:
                issues_data[key].extend([float("nan")] * (max_length - len(value)))

        issues_data.update(issues_data1)

        issues_data = pd.DataFrame(issues_data)
        issues_data["backlog"] = issues_data["criado"]

        # Exibe o dataframe com as informações das issues e durações de status
        fim = time.time()
        print(fim - inicio)

        # replace_types
        replace_dtypes = {
            "projeto": "string",
            "projeto_key": "string",
            "issue_id": "string",
            "prioridade": "string"
            
        }

        
        print("SUCCESS")
