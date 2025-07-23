import os
from pandas_gbq import read_gbq

# ID de tu proyecto en Google Cloud
PROJECT_ID = "practicas-456510"

def load_sql(file_name):
    """
    Carga un archivo SQL desde analysis/sql/, funciona tanto en Colab como en VS Code.
    """
    try:
        # Si estamos ejecutando como script (VS Code)
        base_path = os.path.dirname(__file__)
    except NameError:
        # Si estamos en un entorno como Colab (__file__ no está definido)
        base_path = os.getcwd()

    # Ruta completa al archivo .sql
    sql_path = os.path.join(base_path, "analysis", "sql", file_name)

    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Archivo SQL no encontrado: {sql_path}")

    with open(sql_path, "r") as file:
        return file.read()


# ======================
# FUNCIONES DISPONIBLES
# ======================

def get_total_estancias_uci():
    query = load_sql("estancias_uci.sql")
    return read_gbq(query, project_id=PROJECT_ID)


def get_estancias_uci_detalle_preview():
    query = load_sql("estancias_uci_detalle.sql")
    return read_gbq(query, project_id=PROJECT_ID)


def get_estancias_uci_cultivos():
    query = load_sql("estancias_uci_cultivos.sql")
    return read_gbq(query, project_id=PROJECT_ID)

# ⚠️ Puedes seguir agregando más funciones así:
# def get_nombre_funcion():
#     query = load_sql("nombre_del_sql.sql")
#     return read_gbq(query, project_id=PROJECT_ID)
