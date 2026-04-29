-- proposito:
--   Auditar la distribucion de dominios clinicos y flags de mejoria.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
--   - `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
--
-- nivel de agregacion:
--   - Una fila por variable y valor observado.
--
-- claves esperadas:
--   - subject_id, hadm_id, stay_id, day_idx.
--
-- posibles errores detectables:
--   - Outcomes constantes.
--   - Valores fuera de rango esperado para flags binarios.
--   - Ausencia de mejoria sostenida.
--   - Distribucion inesperada de `n_domains_ok`.

WITH distributions AS (
  SELECT 'clinical_domains_sci_clean' AS table_name, 'no_new_foci_flag' AS variable_name, CAST(no_new_foci_flag AS STRING) AS value, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY value

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'radiology_stable_flag', CAST(radiology_stable_flag AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'temp_in_range', CAST(temp_in_range AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'wbc_normalizing', CAST(wbc_normalizing AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'hemo_stable', CAST(hemo_stable AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'lactate_normalizing', CAST(lactate_normalizing AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'clinical_domains_sci_clean', 'resp_improving', CAST(resp_improving AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'improvement_flags_clean', 'n_domains_ok', CAST(n_domains_ok AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'improvement_flags_clean', 'improved_today', CAST(improved_today AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  GROUP BY 3

  UNION ALL
  SELECT 'improvement_flags_clean', 'sustained_improvement', CAST(sustained_improvement AS STRING), COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  GROUP BY 3
)

SELECT
  table_name,
  variable_name,
  value,
  n_rows
FROM distributions
ORDER BY table_name, variable_name, value;
