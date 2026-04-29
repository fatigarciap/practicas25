import os

from pandas_gbq import read_gbq


# ID de tu proyecto en Google Cloud para ejecutar las consultas desde Colab/VS Code.
PROJECT_ID = "practicas-456510"


def load_sql(file_name):
    """
    Busca un archivo SQL dentro de cualquier subcarpeta de 'analysis'.
    Funciona tanto en Colab como en VS Code.
    """
    try:
        base_path = os.path.dirname(__file__)
    except NameError:
        base_path = os.getcwd()

    analysis_path = os.path.join(base_path, "analysis")

    for root, _dirs, files in os.walk(analysis_path):
        if file_name in files:
            sql_path = os.path.join(root, file_name)
            with open(sql_path, "r", encoding="utf-8") as file:
                return file.read()

    raise FileNotFoundError(
        f"Archivo SQL no encontrado en ninguna subcarpeta de 'analysis': {file_name}"
    )


def run_sql_file(file_name):
    query = load_sql(file_name)
    return read_gbq(query, project_id=PROJECT_ID)


# ==========================
# PIPELINE FINAL - COHORTE
# ==========================

def get_00_01_episode_candidates_clean():
    return run_sql_file("00_01_episode_candidates_clean.sql")


def get_00_02_antibiogram_detail_clean():
    return run_sql_file("00_02_antibiogram_detail_clean.sql")


def get_00_03_index_stay_clean():
    return run_sql_file("00_03_index_stay_clean.sql")


# ===============================
# PIPELINE FINAL - ANTIBIOTICOS/T0
# ===============================

def get_01_01_abx_spectrum_map_clean():
    return run_sql_file("01_01_abx_spectrum_map_clean.sql")


def get_01_01_t0_true():
    return run_sql_file("01_01_t0_true.sql")


def get_01_03_baseline_regimen_detail_clean():
    return run_sql_file("01_03_baseline_regimen_detail_clean.sql")


def get_01_04_baseline_regimen_summary_clean():
    return run_sql_file("01_04_baseline_regimen_summary_clean.sql")


def get_01_05_baseline_regimen_multihot_clean():
    return run_sql_file("01_05_baseline_regimen_multihot_clean.sql")


# =========================
# PIPELINE FINAL - VENTANAS
# =========================

def get_02_01_base_windows_clean():
    return run_sql_file("02_01_base_windows_clean.sql")


# =======================
# PIPELINE FINAL - EVENTOS
# =======================

def get_03_01_radiology_worsening_events_clean():
    return run_sql_file("03_01_radiology_worsening_events_clean.sql")


def get_03_02_radiology_flag_clean():
    return run_sql_file("03_02_radiology_flag_clean.sql")


def get_03_03_new_foci_events_clean():
    return run_sql_file("03_03_new_foci_events_clean.sql")


def get_03_04_new_foci_flag_clean():
    return run_sql_file("03_04_new_foci_flag_clean.sql")


# ================================
# PIPELINE FINAL - VARIABLES DIARIAS
# ================================

def get_04_01_daily_features_clean():
    return run_sql_file("04_01_daily_features_clean.sql")


# ========================
# PIPELINE FINAL - OUTCOME
# ========================

def get_05_01_clinical_domains_sci_clean():
    return run_sql_file("05_01_clinical_domains_sci_clean.sql")


def get_05_02_improvement_flags_clean():
    return run_sql_file("05_02_improvement_flags_clean.sql")


# ============================
# PIPELINE FINAL - TABLA FINAL
# ============================

def get_06_01_longitudinal_cohort_model_ready():
    return run_sql_file("06_01_longitudinal_cohort_model_ready.sql")


# ======================
# QC - CONTROLES CALIDAD
# ======================

def get_checks_iniciales_calidad():
    """
    Ejecuta el resumen inicial de calidad de la tabla longitudinal final.

    Ejemplo en Colab:
        df = get_checks_iniciales_calidad()
        display(df)
    """
    return run_sql_file("qc_checks_iniciales_calidad.sql")


def get_qc_00_list_tables():
    return run_sql_file("qc_00_list_tables.sql")


def get_qc_01_row_counts():
    return run_sql_file("qc_01_row_counts.sql")


def get_qc_02_cohort_flow():
    return run_sql_file("qc_02_cohort_flow.sql")


def get_qc_03_key_integrity():
    return run_sql_file("qc_03_key_integrity.sql")


def get_qc_04_missingness():
    return run_sql_file("qc_04_missingness.sql")


def get_qc_05_outcome_distribution():
    return run_sql_file("qc_05_outcome_distribution.sql")


def get_qc_06_temporal_sanity():
    return run_sql_file("qc_06_temporal_sanity.sql")


def get_qc_longitudinal_full_validation():
    return run_sql_file("qc_longitudinal_full_validation.sql")

