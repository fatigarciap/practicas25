-- proposito:
--   Estimar missingness de variables clinicas diarias y dominios de outcome.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.daily_features_clean`
--   - `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
--   - `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
--
-- nivel de agregacion:
--   - Una fila por variable auditada.
--
-- claves esperadas:
--   - subject_id, hadm_id, stay_id, day_idx.
--
-- posibles errores detectables:
--   - Variables completamente ausentes.
--   - Missingness elevada por itemids incorrectos o ventanas sin mediciones.
--   - Dominios clinicos no calculados por falta de features.

WITH variable_missingness AS (
  SELECT 'daily_features_clean' AS table_name, 'HR_median' AS variable_name, COUNT(*) AS n_rows, COUNTIF(HR_median IS NULL) AS n_missing FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'MAP_median', COUNT(*), COUNTIF(MAP_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'SysBP_median', COUNT(*), COUNTIF(SysBP_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'DiasBP_median', COUNT(*), COUNTIF(DiasBP_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Temp_median', COUNT(*), COUNTIF(Temp_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'RR_median', COUNT(*), COUNTIF(RR_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'SpO2_median', COUNT(*), COUNTIF(SpO2_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'FiO2_median', COUNT(*), COUNTIF(FiO2_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'WBC_median', COUNT(*), COUNTIF(WBC_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Lactate_median', COUNT(*), COUNTIF(Lactate_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Creatinine_median', COUNT(*), COUNTIF(Creatinine_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Bilirubin_median', COUNT(*), COUNTIF(Bilirubin_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Platelets_median', COUNT(*), COUNTIF(Platelets_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'Hgb_median', COUNT(*), COUNTIF(Hgb_median IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  UNION ALL SELECT 'daily_features_clean', 'spo2fio2_ratio', COUNT(*), COUNTIF(spo2fio2_ratio IS NULL) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`

  UNION ALL SELECT 'clinical_domains_sci_clean', 'no_new_foci_flag', COUNT(*), COUNTIF(no_new_foci_flag IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'radiology_stable_flag', COUNT(*), COUNTIF(radiology_stable_flag IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'temp_in_range', COUNT(*), COUNTIF(temp_in_range IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'wbc_normalizing', COUNT(*), COUNTIF(wbc_normalizing IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'hemo_stable', COUNT(*), COUNTIF(hemo_stable IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'lactate_normalizing', COUNT(*), COUNTIF(lactate_normalizing IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  UNION ALL SELECT 'clinical_domains_sci_clean', 'resp_improving', COUNT(*), COUNTIF(resp_improving IS NULL) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`

  UNION ALL SELECT 'improvement_flags_clean', 'n_domains_ok', COUNT(*), COUNTIF(n_domains_ok IS NULL) FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  UNION ALL SELECT 'improvement_flags_clean', 'improved_today', COUNT(*), COUNTIF(improved_today IS NULL) FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  UNION ALL SELECT 'improvement_flags_clean', 'sustained_improvement', COUNT(*), COUNTIF(sustained_improvement IS NULL) FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
)

SELECT
  table_name,
  variable_name,
  n_rows,
  n_missing,
  SAFE_DIVIDE(n_missing, n_rows) AS missing_fraction
FROM variable_missingness
ORDER BY table_name, variable_name;
