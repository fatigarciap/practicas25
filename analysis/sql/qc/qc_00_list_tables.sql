-- proposito:
--   Inventariar las tablas existentes del dataset `mimic_analysis` que forman parte
--   del pipeline auditado.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.INFORMATION_SCHEMA.TABLES`
--
-- nivel de agregacion:
--   - Una fila por tabla encontrada en el dataset.
--
-- claves esperadas:
--   - table_name.
--
-- posibles errores detectables:
--   - Tablas canonicas ausentes.
--   - Presencia simultanea de versiones `_clean` y no `_clean`.
--   - Tablas finales o intermedias no materializadas antes de ejecutar pasos posteriores.

WITH expected_tables AS (
  SELECT 'bloque_0_episode_candidates_clean' AS table_name UNION ALL
  SELECT 'bloque_0_antibiogram_detail_clean' UNION ALL
  SELECT 'bloque_0b_index_stay_clean' UNION ALL
  SELECT 'abx_spectrum_map_clean' UNION ALL
  SELECT 'bloque_t0_true' UNION ALL
  SELECT 'baseline_regimen_detail_clean' UNION ALL
  SELECT 'baseline_regimen_summary_clean' UNION ALL
  SELECT 'baseline_regimen_multihot_clean' UNION ALL
  SELECT 'bloque_1_base_windows_clean' UNION ALL
  SELECT 'radiology_worsening_events_clean' UNION ALL
  SELECT 'radiology_flag_clean' UNION ALL
  SELECT 'new_foci_events_clean' UNION ALL
  SELECT 'new_foci_flag_clean' UNION ALL
  SELECT 'daily_features_clean' UNION ALL
  SELECT 'clinical_domains_sci_clean' UNION ALL
  SELECT 'improvement_flags_clean' UNION ALL
  SELECT 'longitudinal_cohort_model_ready'
),
existing_tables AS (
  SELECT
    table_name,
    table_type,
    creation_time
  FROM `strange-math-456415-c3.mimic_analysis.INFORMATION_SCHEMA.TABLES`
)

SELECT
  e.table_name,
  CASE WHEN t.table_name IS NULL THEN 0 ELSE 1 END AS table_exists,
  t.table_type,
  t.creation_time
FROM expected_tables e
LEFT JOIN existing_tables t
USING (table_name)
ORDER BY e.table_name;
