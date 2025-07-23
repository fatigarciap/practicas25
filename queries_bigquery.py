import os
from pandas_gbq import read_gbq

PROJECT_ID = "practicas-456510"

def load_sql(file_name):
    sql_path = os.path.join(os.path.dirname(__file__), 'analysis', 'sql', file_name)
    with open(sql_path, 'r') as file:
        return file.read()

def get_total_estancias_uci():
    query = load_sql("estancias_uci.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_detalle_preview():
    query = load_sql("estancias_uci_detalle.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_cultivos():
    query = load_sql("estancias_uci_cultivos.sql")
    return read_gbq(query, project_id=PROJECT_ID)