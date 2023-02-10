import logging
import time
import requests
import psycopg2
from sqlalchemy.orm import declarative_base, Session, sessionmaker
from sqlalchemy import create_engine, text, Table, DateTime, Integer, Column
import datetime
from types import NoneType

engine = create_engine('postgresql://totvsbiservices:EpTLU:?$ep?P(tJ:@35.188.85.47:5432/jira_prometheus')

Base = declarative_base()
class Issue(Base):
    __table__ = Table('issues', Base.metadata, autoload_with=engine)
class Campos(Base):
    __table__ = Table('campos', Base.metadata, autoload_with=engine)
class Projetos(Base):
    __table__ = Table('projetos', Base.metadata, autoload_with=engine)
class Worklog(Base):
    __table__ = Table('worklog', Base.metadata, autoload_with=engine)
    
class Update(Base):
    __tablename__ = 'last_update'
    id = Column(Integer, primary_key=True)
    # outras colunas
    last_update = Column(DateTime, default=datetime.datetime.utcnow)

# Criando as sessões do banco de dados
Session = sessionmaker(bind=engine)
session = Session()
update = Update()

# Configuração do sistema de log
logging.basicConfig(filename='etl.log', level=logging.INFO, format='%(asctime)s - %(message)s')


mapping = {row.nome_api: row.nome_db for row in session.query(Campos).all()}
# Lista de projetos do Jira
projects = [row.projetos for row in session.query(Projetos).all()]

# URL da API do Jira
jira_url = 'https://jiraproducao.totvs.com.br/rest/api/2'
headers = {
    'User-Agent': 'DataStudio Engenharia V&D',
    'Authorization': 'Basic aGVucmlxdWUubW9yZXR0aTpTQG5zdW5nQDEyMyM0NTZAMTIz'
    }

updated_issues = []

def run_etl():
    logging.info("Iniciando ETL process")
    start_time = time.time()
    issues_saved = 0
    worklogs_saved = 0
    # Loop pelos projetos
    for project in projects:
        start_at = 0
        max_results = 500
        keep_running = True

        # Loop pelas issues do projeto
        while keep_running:
            try:
                #busca last_update da tabela update
                last_update = session.query(Update).filter_by(
                        id=1
                    ).first()
                data = last_update.last_update.strftime("%Y-%m-%d %H:%M")
                # Faz uma requisição POST à API do Jira
                response = requests.post(f"{jira_url}/search", headers=headers, json={
                    "jql": f"(project in({project}) AND updated >= '{data}')",
                    "startAt": start_at,
                    "maxResults": max_results,
                    "fields": [],
                })
                total = response.json()["total"]
                startAt = response.json()["startAt"]
                logging.info(f"Projeto: {project} Issues: {total} startAt: {startAt}")
                # Verifica se a resposta da API foi válida
                if response.status_code != 200:
                    logging.error(f"Erro ao acessar a API do Jira para o projeto {project['project_name']}: {response.text}")
                    break            
                # Extrai as informações das issues da resposta da API
                issues = response.json()["issues"]
                # Loop pelas issues
                for issue in issues:
                    # Cria um objeto Issue com as informações da API
                    db_issue = Issue()
                    for api_field, db_field in mapping.items():
                        value = issue
                        for key in api_field.split("."):
                            if type(value) not in [list, type(None)]:
                                value = value.get(key)
                            elif type(value) == list and value:
                                value = value[0]
                                sub_value = value.get(key)
                                if sub_value:
                                    value = sub_value
                                else:
                                    break
                            else:
                                break
                        setattr(db_issue, db_field, value)


                    # Verifica se a versão da issue na API é mais nova que a do banco
                    db_issue_in_db = session.query(Issue).filter_by(
                        issueId=db_issue.issueId
                    ).first()
                    if db_issue_in_db is None:
                        session.add(db_issue)
                        updated_issues.append(issue['key'])
                        issues_saved += 1


                    elif datetime.datetime.strptime(db_issue.lastUpdate, "%Y-%m-%dT%H:%M:%S.%f%z") > db_issue_in_db.lastUpdate:
                        session.delete(db_issue_in_db)
                        session.add(db_issue)
                        updated_issues.append(issue['key'])
                        issues_saved += 1

                # Salva as informações das issues no banco de dados
                session.commit()
                logging.info(f"Foram salvos {issues_saved} issues do projeto: {project}")

                # Verifica se há mais issues a serem acessadas
                start_at += max_results
                if len(issues) < max_results:
                    keep_running = False
            except Exception as e:
                # Registra o erro no log
                logging.error(f"Erro ao acessar as issues do projeto {project['project_name']}: {str(e)}")
                keep_running = False
            
            # Loop pelas issues atualizadas
            for issue in updated_issues:
                # Faz uma requisição GET para o endpoint worklog da API do Jira
                response = requests.get(f"{jira_url}/issue/{issue}/worklog", headers=headers)
                if response.status_code == 200:
                    worklogs = response.json()["worklogs"]
                    for worklog in worklogs:
                        # Cria um objeto Worklog com as informações da API
                        db_worklog = Worklog(
                            issueKey=issue,
                            worklog_id=worklog["id"],
                            author=worklog["author"]["name"],
                            started=worklog["started"],
                            created=worklog["created"],
                            updated=worklog["updated"],
                            TimeSpentSegundos=worklog["timeSpentSeconds"],
                            TimeSpent=worklog["timeSpent"],
                            comments=worklog.get("comment")
                        )

                        # Verifica se a versão do worklog na API é mais nova que a do banco
                        db_worklog_in_db = session.query(Worklog).filter_by(
                            worklog_id=db_worklog.worklog_id
                        ).first()
                        if db_worklog_in_db is None:
                            session.add(db_worklog)
                            worklogs_saved += 1

                        elif datetime.datetime.strptime(db_worklog.updated, "%Y-%m-%dT%H:%M:%S.%f%z") > db_worklog_in_db.updated:
                            session.delete(db_worklog_in_db)
                            session.add(db_worklog)
                            worklogs_saved += 1


                    # Salva as informações dos worklogs no banco de dados
                    session.add(update)
                    session.commit()
                else:
                    # Registra o erro no log
                    logging.error(f"Erro ao acessar o worklog da issue {issue}: {response.json()}")

    # Fecha a sessão com o banco de dados
    session.close()
    
    end_time = time.time()
    logging.info(f"ETL process finalizado. Foram salvos {issues_saved} issues e {worklogs_saved} worklogs em {end_time - start_time} segundos")


run_etl()
