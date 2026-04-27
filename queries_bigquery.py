import os
from pandas_gbq import read_gbq

PROJECT_ID = "practicas-456510"


def load_sql(relative_path):
    """
    Carga archivos SQL desde:
    analysis/pipeline_final/
    """
    try:
        base_path = os.path.dirname(__file__)
    except NameError:
        base_path = os.getcwd()

    full_path = os.path.join(
        base_path,
        "analysis",
        "pipeline_final",
        relative_path
    )

    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Archivo SQL no encontrado: {full_path}")

    with open(full_path, "r", encoding="utf-8") as file:
        return file.read()


def run_sql(relative_path):
    query = load_sql(relative_path)
    return read_gbq(query, project_id=PROJECT_ID)


# ======================
# BLOQUE 0 — COHORTE
# ======================

def get_00_01_episode_candidates_clean():
    return run_sql("00_cohort/00_01_episode_candidates_clean.sql")


def get_00_02_antibiogram_detail_clean():
    return run_sql("00_cohort/00_02_antibiogram_detail_clean.sql")


def get_00_03_index_stay_clean():
    return run_sql("00_cohort/00_03_index_stay_clean.sql")


# ======================
# BLOQUE 1 — T0 Y ANTIBIÓTICOS
# ======================

def get_01_01_abx_spectrum_map_clean():
    return run_sql("01_antibiotics_t0/01_01_abx_spectrum_map_clean.sql")


def get_01_02_t0_true():
    return run_sql("01_antibiotics_t0/01_02_t0_true.sql")


def get_01_03_baseline_regimen_detail_clean():
    return run_sql("01_antibiotics_t0/01_03_baseline_regimen_detail_clean.sql")


def get_01_04_baseline_regimen_summary_clean():
    return run_sql("01_antibiotics_t0/01_04_baseline_regimen_summary_clean.sql")


def get_01_05_baseline_regimen_multihot_clean():
    return run_sql("01_antibiotics_t0/01_05_baseline_regimen_multihot_clean.sql")


# ======================
# BLOQUE 2 — VENTANAS
# ======================

def get_02_01_base_windows_clean():
    return run_sql("02_windows/02_01_base_windows_clean.sql")


# ======================
# BLOQUE 3 — EVENTOS
# ======================

def get_03_01_radiology_worsening_events_clean():
    return run_sql("03_events/03_01_radiology_worsening_events_clean.sql")


def get_03_02_radiology_flag_clean():
    return run_sql("03_events/03_02_radiology_flag_clean.sql")


def get_03_03_new_foci_events_clean():
    return run_sql("03_events/03_03_new_foci_events_clean.sql")


def get_03_04_new_foci_flag_clean():
    return run_sql("03_events/03_04_new_foci_flag_clean.sql")


# ======================
# BLOQUE 4 — VARIABLES DIARIAS
# ======================

def get_04_01_daily_features_clean():
    return run_sql("04_daily_features/04_01_daily_features_clean.sql")


# ======================
# BLOQUE 5 — OUTCOME
# ======================

def get_05_01_clinical_domains_sci_clean():
    return run_sql("05_outcome/05_01_clinical_domains_sci_clean.sql")


def get_05_02_improvement_flags_clean():
    return run_sql("05_outcome/05_02_improvement_flags_clean.sql")


# ======================
# BLOQUE 6 — TABLA FINAL
# ======================

def get_06_01_longitudinal_cohort_model_ready():
    return run_sql("06_final_table/06_01_longitudinal_cohort_model_ready.sql")


# ======================
# EJECUCIÓN COMPLETA DEL PIPELINE
# ======================

def run_full_pipeline():
    """
    Ejecuta todo el pipeline en orden.
    Úsalo cuando todos los SQL sean CREATE OR REPLACE TABLE.
    """

    print("Ejecutando BLOQUE 0 — Cohorte")
    get_00_01_episode_candidates_clean()
    get_00_02_antibiogram_detail_clean()
    get_00_03_index_stay_clean()

    print("Ejecutando BLOQUE 1 — T0 y régimen antibiótico")
    get_01_01_abx_spectrum_map_clean()
    get_01_02_t0_true()
    get_01_03_baseline_regimen_detail_clean()
    get_01_04_baseline_regimen_summary_clean()
    get_01_05_baseline_regimen_multihot_clean()

    print("Ejecutando BLOQUE 2 — Ventanas")
    get_02_01_base_windows_clean()

    print("Ejecutando BLOQUE 3 — Eventos")
    get_03_01_radiology_worsening_events_clean()
    get_03_02_radiology_flag_clean()
    get_03_03_new_foci_events_clean()
    get_03_04_new_foci_flag_clean()

    print("Ejecutando BLOQUE 4 — Variables clínicas diarias")
    get_04_01_daily_features_clean()

    print("Ejecutando BLOQUE 5 — Outcome")
    get_05_01_clinical_domains_sci_clean()
    get_05_02_improvement_flags_clean()

    print("Ejecutando BLOQUE 6 — Tabla final")
    get_06_01_longitudinal_cohort_model_ready()

    print("Pipeline ejecutado correctamente.")