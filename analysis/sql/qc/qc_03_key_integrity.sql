-- proposito:
--   Auditar unicidad y duplicados en claves esperadas por etapa.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
--   - `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
--   - `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
--   - `strange-math-456415-c3.mimic_analysis.daily_features_clean`
--   - `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
--   - `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
--   - `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
--
-- nivel de agregacion:
--   - Una fila por tabla y clave auditada.
--
-- claves esperadas:
--   - `stay_id` para tablas de una fila por estancia.
--   - `stay_id, day_idx` para tablas longitudinales diarias.
--
-- posibles errores detectables:
--   - Duplicados por estancia tras seleccion indice.
--   - Duplicados por dia que inflarian joins longitudinales.
--   - Multiplicacion de filas en la tabla final.

WITH checks AS (
  SELECT
    'bloque_0b_index_stay_clean' AS table_name,
    'stay_id' AS expected_key,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT CAST(stay_id AS STRING)) AS n_distinct_keys
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`

  UNION ALL
  SELECT 'bloque_t0_true', 'stay_id', COUNT(*), COUNT(DISTINCT CAST(stay_id AS STRING))
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT 'baseline_regimen_summary_clean', 'stay_id', COUNT(*), COUNT(DISTINCT CAST(stay_id AS STRING))
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`

  UNION ALL
  SELECT 'baseline_regimen_multihot_clean', 'stay_id', COUNT(*), COUNT(DISTINCT CAST(stay_id AS STRING))
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`

  UNION ALL
  SELECT 'bloque_1_base_windows_clean', 'stay_id, day_idx', COUNT(*), COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING)))
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`

  UNION ALL
  SELECT 'daily_features_clean', 'stay_id, day_idx', COUNT(*), COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING)))
  FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'stay_id, day_idx', COUNT(*), COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING)))
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`

  UNION ALL
  SELECT 'improvement_flags_clean', 'stay_id, day_idx', COUNT(*), COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING)))
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`

  UNION ALL
  SELECT 'longitudinal_cohort_model_ready', 'stay_id, day_idx', COUNT(*), COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING)))
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)

SELECT
  table_name,
  expected_key,
  n_rows,
  n_distinct_keys,
  n_rows - n_distinct_keys AS n_duplicate_key_rows,
  CASE WHEN n_rows = n_distinct_keys THEN 1 ELSE 0 END AS key_unique_flag
FROM checks
ORDER BY table_name;
