import os
from pandas_gbq import read_gbq

# ID de tu proyecto en Google Cloud
PROJECT_ID = "practicas-456510"

def load_sql(file_name):
    """
    Busca un archivo SQL dentro de cualquier subcarpeta de 'analysis'.
    Funciona tanto en Colab como en VS Code.
    """
    try:
        # Si estamos ejecutando como script (VS Code)
        base_path = os.path.dirname(__file__)
    except NameError:
        # Si estamos en Colab (__file__ no está definido)
        base_path = os.getcwd()

    analysis_path = os.path.join(base_path, "analysis")

    # Recorrer todas las subcarpetas dentro de 'analysis'
    for root, dirs, files in os.walk(analysis_path):
        if file_name in files:
            sql_path = os.path.join(root, file_name)
            with open(sql_path, "r") as file:
                return file.read()

    # Si no se encontró el archivo
    raise FileNotFoundError(f"Archivo SQL no encontrado en ninguna subcarpeta de 'analysis': {file_name}")


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

def get_recuento_estancias_uci():
    query = load_sql("recuento_estancias_uci.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_microevents():
    query = load_sql("estancias_uci_microevents.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_monoinfeccion():
    query = load_sql("estancias_uci_monoinfeccion.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_monoinfeccion_filtrada():
    query = load_sql("estancias_uci_monoinfeccion_filtrada.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_monoinfeccion_filtrada_comorb():
    query = load_sql("estancias_uci_monoinfeccion_filtrada_comorb.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_monoinfeccion_con_tratamiento_previo():
    query = load_sql("estancias_uci_monoinfeccion_con_tratamiento_previo.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_conteo_estancias_uci_monoinfeccion():
    query = load_sql("conteo_estancias_uci_monoinfeccion.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_48h_tratamiento():
    query = load_sql("estancias_uci_48h_tratamiento.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_bloques_monomicrobianos():
    query = load_sql("estancias_uci_bloques_monomicrobianos.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_estancias_uci_metrica_clinica():
    query = load_sql("estancias_uci_metrica_clinica.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_b1_base_windows():
    query = load_sql("b1_base_windows.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_b2_dailyfeatures():
    query = load_sql("b2_dailyfeatures.sql")
    return read_gbq(query, project_id=PROJECT_ID)

def get_b3_clinicaldomains():
    query = load_sql("b3_clinicaldomains.sql")
    return read_gbq(query, project_id=PROJECT_ID)
def get_b4_improvementsflags():
    query = load_sql("b4_improvementsflags.sql")
    return read_gbq(query, project_id=PROJECT_ID)
def get_longitudinal_cohort():
    query = load_sql("pipeline_final/longitudinal_cohort.sql")
    return read_gbq(query, project_id=PROJECT_ID)

# ⚠️ Puedes seguir agregando más funciones así:
# def get_nombre_funcion():
#     query = load_sql("nombre_del_sql.sql")
#     return read_gbq(query, project_id=PROJECT_ID)
