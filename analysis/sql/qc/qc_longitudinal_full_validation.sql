-- qc_longitudinal_full_validation.sql
-- BigQuery Standard SQL
--
-- Objetivo:
--   Validar tabla longitudinal final y tablas intermedias del pipeline amr_days/MIMIC-IV.
--
-- Instrucciones:
--   1) Ejecutar por bloques o como script BigQuery.
--   2) Si tu dataset cambia, reemplaza:
--        `strange-math-456415-c3.mimic_analysis`
--      por:
--        `PROJECT_ID.DATASET_ID`
--
-- Tablas esperadas:
--   longitudinal_cohort_model_ready
--   bloque_0b_index_stay_clean
--   bloque_t0_true
--   baseline_regimen_detail_clean
--   baseline_regimen_summary_clean
--   baseline_regimen_multihot_clean
--   bloque_1_base_windows_clean
--   daily_features_clean
--   clinical_domains_sci_clean
--   improvement_flags_clean
--   new_foci_events_clean
--   new_foci_flag_clean
--   radiology_worsening_events_clean
--   radiology_flag_clean

-- ============================================================================
-- 0. Row counts generales
-- ============================================================================

SELECT 'longitudinal_cohort_model_ready' AS table_name, COUNT(*) AS n_rows
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
UNION ALL SELECT 'bloque_0b_index_stay_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
UNION ALL SELECT 'bloque_t0_true', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
UNION ALL SELECT 'baseline_regimen_detail_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
UNION ALL SELECT 'baseline_regimen_summary_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
UNION ALL SELECT 'baseline_regimen_multihot_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
UNION ALL SELECT 'bloque_1_base_windows_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
UNION ALL SELECT 'daily_features_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
UNION ALL SELECT 'clinical_domains_sci_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
UNION ALL SELECT 'improvement_flags_clean', COUNT(*)
FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`;

-- ============================================================================
-- 1. Unicidad de claves
-- ============================================================================

-- 1.1 Tabla longitudinal: una fila por stay_id + day_idx.
SELECT
  'longitudinal_unique_stay_day' AS check_name,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING))) AS n_distinct_keys,
  COUNT(*) - COUNT(DISTINCT CONCAT(CAST(stay_id AS STRING), '|', CAST(day_idx AS STRING))) AS n_duplicate_rows
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`;

-- 1.2 Episodio indice: una fila por stay_id.
SELECT
  'index_unique_stay' AS check_name,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT stay_id) AS n_distinct_stays,
  COUNT(*) - COUNT(DISTINCT stay_id) AS n_duplicate_rows
FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`;

-- ============================================================================
-- 2. Duplicados
-- ============================================================================

-- 2.1 Duplicados por stay_id en tablas que deberian ser una fila por estancia.
SELECT table_name, stay_id, n_rows
FROM (
  SELECT 'bloque_0b_index_stay_clean' AS table_name, stay_id, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
  GROUP BY stay_id
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'bloque_t0_true' AS table_name, stay_id, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
  GROUP BY stay_id
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'baseline_regimen_summary_clean' AS table_name, stay_id, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
  GROUP BY stay_id
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'baseline_regimen_multihot_clean' AS table_name, stay_id, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
  GROUP BY stay_id
  HAVING COUNT(*) > 1
)
ORDER BY table_name, n_rows DESC;

-- 2.2 Duplicados por stay_id + day_idx en tablas diarias.
SELECT table_name, stay_id, day_idx, n_rows
FROM (
  SELECT 'longitudinal_cohort_model_ready' AS table_name, stay_id, day_idx, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'bloque_1_base_windows_clean' AS table_name, stay_id, day_idx, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'daily_features_clean' AS table_name, stay_id, day_idx, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'clinical_domains_sci_clean' AS table_name, stay_id, day_idx, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT 'improvement_flags_clean' AS table_name, stay_id, day_idx, COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1
)
ORDER BY table_name, stay_id, day_idx;

-- 2.3 Duplicados por microevent_id en episodio indice.
SELECT
  microevent_id,
  COUNT(*) AS n_rows,
  COUNT(DISTINCT stay_id) AS n_stays,
  ARRAY_AGG(DISTINCT stay_id ORDER BY stay_id LIMIT 10) AS example_stay_ids
FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
GROUP BY microevent_id
HAVING COUNT(*) > 1
ORDER BY n_rows DESC, microevent_id;

-- ============================================================================
-- 3. Coherencia temporal
-- ============================================================================

-- 3.1 Resumen de violaciones temporales en tabla final.
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(t0 < icu_intime) AS n_t0_before_icu_intime,
  COUNTIF(t0 > icu_outtime) AS n_t0_after_icu_outtime,
  COUNTIF(window_start >= window_end) AS n_invalid_window_order,
  COUNTIF(window_start < t0) AS n_window_start_before_t0,
  COUNTIF(followup_end > icu_outtime) AS n_followup_after_icu_outtime,
  COUNTIF(deathtime IS NOT NULL AND followup_end > deathtime) AS n_followup_after_death,
  COUNTIF(followup_end > TIMESTAMP_ADD(t0, INTERVAL 30 DAY)) AS n_followup_after_30d,
  COUNTIF(window_end > followup_end) AS n_window_end_after_followup_end
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`;

-- 3.2 Filas concretas con incoherencias temporales.
SELECT
  subject_id,
  hadm_id,
  stay_id,
  microevent_id,
  day_idx,
  t0,
  index_charttime,
  icu_intime,
  icu_outtime,
  deathtime,
  followup_end,
  window_start,
  window_end,
  CASE
    WHEN t0 < icu_intime THEN 't0_before_icu_intime'
    WHEN t0 > icu_outtime THEN 't0_after_icu_outtime'
    WHEN window_start >= window_end THEN 'window_start_not_before_window_end'
    WHEN window_start < t0 THEN 'window_start_before_t0'
    WHEN followup_end > icu_outtime THEN 'followup_after_icu_outtime'
    WHEN deathtime IS NOT NULL AND followup_end > deathtime THEN 'followup_after_death'
    WHEN followup_end > TIMESTAMP_ADD(t0, INTERVAL 30 DAY) THEN 'followup_after_30d'
    WHEN window_end > followup_end THEN 'window_end_after_followup_end'
  END AS violation_type
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE t0 < icu_intime
   OR t0 > icu_outtime
   OR window_start >= window_end
   OR window_start < t0
   OR followup_end > icu_outtime
   OR (deathtime IS NOT NULL AND followup_end > deathtime)
   OR followup_end > TIMESTAMP_ADD(t0, INTERVAL 30 DAY)
   OR window_end > followup_end
ORDER BY stay_id, day_idx
LIMIT 500;

-- 3.3 Distancia temporal entre cultivo indice y T0.
SELECT
  COUNT(*) AS n_stays,
  MIN(TIMESTAMP_DIFF(t0, index_charttime, HOUR)) AS min_hours_t0_minus_index,
  APPROX_QUANTILES(TIMESTAMP_DIFF(t0, index_charttime, HOUR), 100)[OFFSET(25)] AS p25_hours,
  APPROX_QUANTILES(TIMESTAMP_DIFF(t0, index_charttime, HOUR), 100)[OFFSET(50)] AS median_hours,
  APPROX_QUANTILES(TIMESTAMP_DIFF(t0, index_charttime, HOUR), 100)[OFFSET(75)] AS p75_hours,
  MAX(TIMESTAMP_DIFF(t0, index_charttime, HOUR)) AS max_hours_t0_minus_index,
  COUNTIF(t0 < index_charttime) AS n_t0_before_culture,
  COUNTIF(t0 = index_charttime) AS n_t0_same_as_culture,
  COUNTIF(t0 > index_charttime) AS n_t0_after_culture
FROM (
  SELECT DISTINCT stay_id, t0, index_charttime
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
);

-- ============================================================================
-- 4. Seguimiento
-- ============================================================================

-- 4.1 Numero de dias observados por paciente/estancia.
WITH per_stay AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    MIN(day_idx) AS min_day_idx,
    MAX(day_idx) AS max_day_idx,
    COUNT(*) AS n_observed_days,
    ANY_VALUE(t0) AS t0,
    ANY_VALUE(icu_outtime) AS icu_outtime,
    ANY_VALUE(deathtime) AS deathtime,
    ANY_VALUE(followup_end) AS followup_end
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  GROUP BY subject_id, hadm_id, stay_id
)
SELECT
  COUNT(*) AS n_stays,
  MIN(n_observed_days) AS min_days,
  APPROX_QUANTILES(n_observed_days, 100)[OFFSET(25)] AS p25_days,
  APPROX_QUANTILES(n_observed_days, 100)[OFFSET(50)] AS median_days,
  APPROX_QUANTILES(n_observed_days, 100)[OFFSET(75)] AS p75_days,
  MAX(n_observed_days) AS max_days,
  COUNTIF(max_day_idx >= 29) AS n_with_day_29_or_more,
  COUNTIF(max_day_idx >= 30) AS n_with_day_30_or_more
FROM per_stay;

-- 4.2 Motivo probable de truncamiento del seguimiento.
WITH per_stay AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    ANY_VALUE(t0) AS t0,
    ANY_VALUE(icu_outtime) AS icu_outtime,
    ANY_VALUE(deathtime) AS deathtime,
    ANY_VALUE(followup_end) AS followup_end,
    MAX(day_idx) AS max_day_idx,
    COUNT(*) AS n_observed_days
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  GROUP BY subject_id, hadm_id, stay_id
),
classified AS (
  SELECT
    *,
    CASE
      WHEN deathtime IS NOT NULL AND followup_end = deathtime THEN 'death'
      WHEN followup_end = icu_outtime THEN 'icu_discharge'
      WHEN followup_end = TIMESTAMP_ADD(t0, INTERVAL 30 DAY) THEN 'administrative_30d'
      ELSE 'other_or_tie'
    END AS followup_stop_reason,
    icu_outtime > TIMESTAMP_ADD(t0, INTERVAL 30 DAY) AS still_in_icu_at_30d
  FROM per_stay
)
SELECT
  followup_stop_reason,
  still_in_icu_at_30d,
  COUNT(*) AS n_stays,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_stays
FROM classified
GROUP BY followup_stop_reason, still_in_icu_at_30d
ORDER BY n_stays DESC;

-- 4.3 Pacientes truncados por muerte.
SELECT DISTINCT
  subject_id,
  hadm_id,
  stay_id,
  t0,
  deathtime,
  followup_end,
  TIMESTAMP_DIFF(deathtime, t0, HOUR) AS hours_t0_to_death
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE deathtime IS NOT NULL
  AND followup_end = deathtime
ORDER BY hours_t0_to_death
LIMIT 500;

-- 4.4 Pacientes truncados por alta de UCI.
SELECT DISTINCT
  subject_id,
  hadm_id,
  stay_id,
  t0,
  icu_outtime,
  followup_end,
  TIMESTAMP_DIFF(icu_outtime, t0, HOUR) AS hours_t0_to_icu_outtime
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE followup_end = icu_outtime
ORDER BY hours_t0_to_icu_outtime
LIMIT 500;

-- 4.5 Pacientes que llegan a 30 dias y/o siguen en UCI al dia 30.
SELECT DISTINCT
  subject_id,
  hadm_id,
  stay_id,
  t0,
  icu_outtime,
  deathtime,
  followup_end,
  icu_outtime > TIMESTAMP_ADD(t0, INTERVAL 30 DAY) AS still_in_icu_at_30d,
  deathtime IS NULL OR deathtime > TIMESTAMP_ADD(t0, INTERVAL 30 DAY) AS alive_at_30d_or_unknown,
  MAX(day_idx) OVER (PARTITION BY stay_id) AS max_day_idx
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE followup_end = TIMESTAMP_ADD(t0, INTERVAL 30 DAY)
ORDER BY stay_id;

-- 4.6 Eventos microbiologicos despues del dia 30 en pacientes de la cohorte.
--     Explora si hay cultivos positivos posteriores a T0+30d durante el mismo ingreso.
WITH cohort AS (
  SELECT DISTINCT subject_id, hadm_id, stay_id, t0
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)
SELECT
  c.subject_id,
  c.hadm_id,
  c.stay_id,
  c.t0,
  CAST(m.charttime AS TIMESTAMP) AS micro_charttime,
  LOWER(m.org_name) AS org_name,
  LOWER(m.spec_type_desc) AS specimen_type,
  TIMESTAMP_DIFF(CAST(m.charttime AS TIMESTAMP), c.t0, DAY) AS days_after_t0
FROM cohort c
JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` m
  ON c.hadm_id = m.hadm_id
WHERE m.charttime IS NOT NULL
  AND m.org_name IS NOT NULL
  AND CAST(m.charttime AS TIMESTAMP) > TIMESTAMP_ADD(c.t0, INTERVAL 30 DAY)
ORDER BY c.stay_id, micro_charttime
LIMIT 500;

-- ============================================================================
-- 5. Outcome
-- ============================================================================

-- 5.1 Proporcion de improved_today y sustained_improvement por fila-dia.
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(improved_today = 1) AS n_improved_today,
  ROUND(100 * COUNTIF(improved_today = 1) / COUNT(*), 2) AS pct_improved_today,
  COUNTIF(sustained_improvement = 1) AS n_sustained_improvement,
  ROUND(100 * COUNTIF(sustained_improvement = 1) / COUNT(*), 2) AS pct_sustained_improvement
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`;

-- 5.2 Proporcion de pacientes/estancias que alcanzan mejoria.
WITH per_stay AS (
  SELECT
    stay_id,
    MAX(COALESCE(improved_today, 0)) AS ever_improved_today,
    MAX(COALESCE(sustained_improvement, 0)) AS ever_sustained_improvement
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  GROUP BY stay_id
)
SELECT
  COUNT(*) AS n_stays,
  COUNTIF(ever_improved_today = 1) AS n_ever_improved_today,
  ROUND(100 * COUNTIF(ever_improved_today = 1) / COUNT(*), 2) AS pct_ever_improved_today,
  COUNTIF(ever_sustained_improvement = 1) AS n_ever_sustained_improvement,
  ROUND(100 * COUNTIF(ever_sustained_improvement = 1) / COUNT(*), 2) AS pct_ever_sustained_improvement
FROM per_stay;

-- 5.3 Primer dia de sustained_improvement por paciente/estancia.
SELECT
  subject_id,
  hadm_id,
  stay_id,
  MIN(day_idx) AS first_sustained_improvement_day,
  MIN(window_start) AS first_sustained_improvement_window_start
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE sustained_improvement = 1
GROUP BY subject_id, hadm_id, stay_id
ORDER BY first_sustained_improvement_day, stay_id;

-- 5.4 Distribucion del primer dia de mejoria sostenida.
WITH first_event AS (
  SELECT
    stay_id,
    MIN(day_idx) AS first_sustained_improvement_day
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  WHERE sustained_improvement = 1
  GROUP BY stay_id
)
SELECT
  first_sustained_improvement_day,
  COUNT(*) AS n_stays
FROM first_event
GROUP BY first_sustained_improvement_day
ORDER BY first_sustained_improvement_day;

-- 5.5 Pacientes que nunca mejoran.
SELECT
  subject_id,
  hadm_id,
  stay_id,
  MIN(day_idx) AS min_day_idx,
  MAX(day_idx) AS max_day_idx,
  ANY_VALUE(t0) AS t0,
  ANY_VALUE(followup_end) AS followup_end,
  ANY_VALUE(deathtime) AS deathtime,
  ANY_VALUE(icu_outtime) AS icu_outtime
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
GROUP BY subject_id, hadm_id, stay_id
HAVING MAX(COALESCE(sustained_improvement, 0)) = 0
ORDER BY max_day_idx DESC, stay_id
LIMIT 500;

-- 5.6 Mejoria despues del dia 30.
--     No evaluable con la tabla final actual, porque las ventanas se truncan en T0+30d.
--     Este check confirma si existen filas mas alla de 30 dias en la tabla final.
SELECT
  'sustained_improvement_after_day_30_in_final_table' AS check_name,
  COUNTIF(day_idx > 30 AND sustained_improvement = 1) AS n_rows_after_day_30_with_sustained_improvement,
  COUNTIF(day_idx > 30) AS n_rows_after_day_30,
  'If both are 0, improvement after 30d is not evaluable without extending windows.' AS interpretation
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`;

-- ============================================================================
-- 6. Antibioticos
-- ============================================================================

-- 6.1 Distribucion de antibioticos basales en T0 desde detalle.
SELECT
  abx_name_std,
  spectrum_level,
  spectrum_label,
  coverage_domain,
  COUNT(*) AS n_regimen_rows,
  COUNT(DISTINCT stay_id) AS n_stays
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
GROUP BY abx_name_std, spectrum_level, spectrum_label, coverage_domain
ORDER BY n_stays DESC, abx_name_std;

-- 6.2 Distribucion de spectrum_level_t0.
SELECT
  spectrum_level_t0,
  COUNT(DISTINCT stay_id) AS n_stays,
  ROUND(100 * COUNT(DISTINCT stay_id) / SUM(COUNT(DISTINCT stay_id)) OVER (), 2) AS pct_stays
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE day_idx = 0
GROUP BY spectrum_level_t0
ORDER BY spectrum_level_t0;

-- 6.3 Pacientes sin antibiotico basal en resumen/final.
SELECT
  subject_id,
  hadm_id,
  stay_id,
  microevent_id,
  t0,
  index_charttime,
  n_abx_t0,
  spectrum_level_t0
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE day_idx = 0
  AND (n_abx_t0 IS NULL OR n_abx_t0 = 0 OR spectrum_level_t0 IS NULL)
ORDER BY stay_id
LIMIT 500;

-- 6.4 Pacientes con multiples antibioticos en T0.
SELECT
  n_abx_t0,
  COUNT(DISTINCT stay_id) AS n_stays
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE day_idx = 0
GROUP BY n_abx_t0
ORDER BY n_abx_t0;

-- 6.5 Detalle de pacientes con multiples antibioticos en T0.
SELECT
  d.subject_id,
  d.hadm_id,
  d.stay_id,
  d.microevent_id,
  d.true_t0,
  STRING_AGG(d.abx_name_std, ', ' ORDER BY d.abx_name_std) AS abx_at_t0,
  COUNT(*) AS n_abx_detail,
  MAX(d.spectrum_level) AS max_spectrum_level
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean` d
GROUP BY d.subject_id, d.hadm_id, d.stay_id, d.microevent_id, d.true_t0
HAVING COUNT(*) > 1
ORDER BY n_abx_detail DESC, stay_id
LIMIT 500;

-- 6.6 Antibiotico iniciado antes del cultivo pero activo en T0.
SELECT
  d.subject_id,
  d.hadm_id,
  d.stay_id,
  d.microevent_id,
  t.index_charttime,
  d.true_t0,
  d.abx_name_std,
  d.start_ts,
  d.stop_ts,
  TIMESTAMP_DIFF(d.start_ts, t.index_charttime, HOUR) AS hours_start_minus_culture
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean` d
JOIN `strange-math-456415-c3.mimic_analysis.bloque_t0_true` t
  USING (subject_id, hadm_id, stay_id, microevent_id)
WHERE d.start_ts < t.index_charttime
  AND d.start_ts <= d.true_t0
  AND (d.stop_ts IS NULL OR d.stop_ts > d.true_t0)
ORDER BY hours_start_minus_culture, stay_id
LIMIT 500;

-- 6.7 Posibles medicamentos no sistemicos en regimen basal.
--     Deberia devolver 0 si se armonizan filtros entre T0 y regimen basal.
SELECT
  d.*
FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean` d
WHERE REGEXP_CONTAINS(LOWER(d.abx_name_std), r'oral|enema|flush|dwell')
ORDER BY stay_id
LIMIT 500;

-- ============================================================================
-- 7. Microbiologia
-- ============================================================================

-- 7.1 Distribucion de microorganismos.
SELECT
  organism_name,
  COUNT(DISTINCT stay_id) AS n_stays,
  COUNT(*) AS n_rows_day0
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE day_idx = 0
GROUP BY organism_name
ORDER BY n_stays DESC, organism_name;

-- 7.2 Distribucion de tipos de muestra.
SELECT
  specimen_type,
  COUNT(DISTINCT stay_id) AS n_stays,
  COUNT(*) AS n_rows_day0
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
WHERE day_idx = 0
GROUP BY specimen_type
ORDER BY n_stays DESC, specimen_type;

-- 7.3 Pacientes con nuevo foco.
WITH per_stay AS (
  SELECT
    l.subject_id,
    l.hadm_id,
    l.stay_id,
    MAX(CASE WHEN l.no_new_foci_flag = 0 THEN 1 ELSE 0 END) AS has_new_focus_in_daily_flag
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` l
  GROUP BY l.subject_id, l.hadm_id, l.stay_id
)
SELECT
  has_new_focus_in_daily_flag,
  COUNT(*) AS n_stays,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_stays
FROM per_stay
GROUP BY has_new_focus_in_daily_flag
ORDER BY has_new_focus_in_daily_flag DESC;

-- 7.4 Tiempo hasta nuevo foco desde T0.
WITH cohort AS (
  SELECT DISTINCT stay_id, hadm_id, t0
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
),
first_new_focus AS (
  SELECT
    n.stay_id,
    MIN(n.charttime) AS first_new_focus_time
  FROM `strange-math-456415-c3.mimic_analysis.new_foci_events_clean` n
  GROUP BY n.stay_id
)
SELECT
  c.stay_id,
  c.hadm_id,
  c.t0,
  f.first_new_focus_time,
  TIMESTAMP_DIFF(f.first_new_focus_time, c.t0, HOUR) AS hours_to_new_focus,
  TIMESTAMP_DIFF(f.first_new_focus_time, c.t0, DAY) AS days_to_new_focus
FROM cohort c
JOIN first_new_focus f
  USING (stay_id)
ORDER BY hours_to_new_focus, stay_id
LIMIT 500;

-- 7.5 Distribucion del tiempo hasta nuevo foco.
WITH cohort AS (
  SELECT DISTINCT stay_id, t0
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
),
first_new_focus AS (
  SELECT
    n.stay_id,
    MIN(n.charttime) AS first_new_focus_time
  FROM `strange-math-456415-c3.mimic_analysis.new_foci_events_clean` n
  GROUP BY n.stay_id
),
times AS (
  SELECT
    TIMESTAMP_DIFF(f.first_new_focus_time, c.t0, DAY) AS days_to_new_focus
  FROM cohort c
  JOIN first_new_focus f
    USING (stay_id)
)
SELECT
  COUNT(*) AS n_stays_with_new_focus,
  MIN(days_to_new_focus) AS min_days,
  APPROX_QUANTILES(days_to_new_focus, 100)[OFFSET(25)] AS p25_days,
  APPROX_QUANTILES(days_to_new_focus, 100)[OFFSET(50)] AS median_days,
  APPROX_QUANTILES(days_to_new_focus, 100)[OFFSET(75)] AS p75_days,
  MAX(days_to_new_focus) AS max_days
FROM times;

-- ============================================================================
-- 8. Missingness
-- ============================================================================

-- 8.1 Missing por variable diaria en la tabla final.
WITH missingness AS (
  SELECT 'HR_median' AS variable_name, COUNT(*) AS n_rows, COUNTIF(HR_median IS NULL) AS n_missing
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'MAP_median', COUNT(*), COUNTIF(MAP_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'SysBP_median', COUNT(*), COUNTIF(SysBP_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'DiasBP_median', COUNT(*), COUNTIF(DiasBP_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Temp_median', COUNT(*), COUNTIF(Temp_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'RR_median', COUNT(*), COUNTIF(RR_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'SpO2_median', COUNT(*), COUNTIF(SpO2_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'FiO2_median', COUNT(*), COUNTIF(FiO2_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'WBC_median', COUNT(*), COUNTIF(WBC_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Lactate_median', COUNT(*), COUNTIF(Lactate_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Creatinine_median', COUNT(*), COUNTIF(Creatinine_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Bilirubin_median', COUNT(*), COUNTIF(Bilirubin_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Platelets_median', COUNT(*), COUNTIF(Platelets_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'Hgb_median', COUNT(*), COUNTIF(Hgb_median IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'spo2fio2_ratio', COUNT(*), COUNTIF(spo2fio2_ratio IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)
SELECT
  variable_name,
  n_rows,
  n_missing,
  n_rows - n_missing AS n_non_missing,
  ROUND(100 * n_missing / n_rows, 2) AS pct_missing
FROM missingness
ORDER BY pct_missing DESC, variable_name;

-- 8.2 Missing por dominios clinicos.
WITH missingness AS (
  SELECT 'no_new_foci_flag' AS variable_name, COUNT(*) AS n_rows, COUNTIF(no_new_foci_flag IS NULL) AS n_missing
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'radiology_stable_flag', COUNT(*), COUNTIF(radiology_stable_flag IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'temp_in_range', COUNT(*), COUNTIF(temp_in_range IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'wbc_normalizing', COUNT(*), COUNTIF(wbc_normalizing IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'hemo_stable', COUNT(*), COUNTIF(hemo_stable IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'lactate_normalizing', COUNT(*), COUNTIF(lactate_normalizing IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
  UNION ALL SELECT 'resp_improving', COUNT(*), COUNTIF(resp_improving IS NULL)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)
SELECT
  variable_name,
  n_rows,
  n_missing,
  n_rows - n_missing AS n_non_missing,
  ROUND(100 * n_missing / n_rows, 2) AS pct_missing
FROM missingness
ORDER BY pct_missing DESC, variable_name;

-- 8.3 Pacientes/dias sin datos fisiologicos suficientes para evaluar outcome.
--     Aqui se considera "suficiente" tener al menos 3 de los 5 dominios fisiologicos no nulos.
WITH daily_eval AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    (
      IF(temp_in_range IS NOT NULL, 1, 0)
      + IF(wbc_normalizing IS NOT NULL, 1, 0)
      + IF(hemo_stable IS NOT NULL, 1, 0)
      + IF(lactate_normalizing IS NOT NULL, 1, 0)
      + IF(resp_improving IS NOT NULL, 1, 0)
    ) AS n_domains_observed,
    n_domains_ok,
    improved_today,
    sustained_improvement
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)
SELECT
  n_domains_observed,
  COUNT(*) AS n_stay_days,
  COUNT(DISTINCT stay_id) AS n_stays,
  COUNTIF(improved_today = 1) AS n_improved_today,
  COUNTIF(sustained_improvement = 1) AS n_sustained_improvement
FROM daily_eval
GROUP BY n_domains_observed
ORDER BY n_domains_observed;

-- 8.4 Estancias sin ningun dia con datos suficientes para evaluar outcome.
WITH daily_eval AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    (
      IF(temp_in_range IS NOT NULL, 1, 0)
      + IF(wbc_normalizing IS NOT NULL, 1, 0)
      + IF(hemo_stable IS NOT NULL, 1, 0)
      + IF(lactate_normalizing IS NOT NULL, 1, 0)
      + IF(resp_improving IS NOT NULL, 1, 0)
    ) AS n_domains_observed
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
),
per_stay AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    MAX(n_domains_observed) AS max_domains_observed_any_day,
    COUNTIF(n_domains_observed >= 3) AS n_days_with_at_least_3_domains
  FROM daily_eval
  GROUP BY subject_id, hadm_id, stay_id
)
SELECT *
FROM per_stay
WHERE n_days_with_at_least_3_domains = 0
ORDER BY max_domains_observed_any_day, stay_id
LIMIT 500;

-- ============================================================================
-- 9. Consistencia entre tablas intermedias y tabla final
-- ============================================================================

-- 9.1 Stays de ventanas base ausentes en tabla final.
SELECT
  b.stay_id,
  COUNT(*) AS n_base_window_rows
FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` b
LEFT JOIN `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` f
  ON b.stay_id = f.stay_id
 AND b.day_idx = f.day_idx
WHERE f.stay_id IS NULL
GROUP BY b.stay_id
ORDER BY n_base_window_rows DESC, b.stay_id
LIMIT 500;

-- 9.2 Filas finales sin daily_features.
SELECT
  f.subject_id,
  f.hadm_id,
  f.stay_id,
  f.day_idx,
  f.window_start,
  f.window_end
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` f
LEFT JOIN `strange-math-456415-c3.mimic_analysis.daily_features_clean` d
  USING (subject_id, hadm_id, stay_id, day_idx)
WHERE d.stay_id IS NULL
ORDER BY f.stay_id, f.day_idx
LIMIT 500;

-- 9.3 Filas finales sin outcome flags limpios.
--     Utiliza las tablas *_clean. Si esta consulta devuelve muchas filas pero la final tiene outcome,
--     probablemente la final se construyo desde tablas antiguas sin sufijo _clean.
SELECT
  f.subject_id,
  f.hadm_id,
  f.stay_id,
  f.day_idx,
  f.improved_today AS final_improved_today,
  c.stay_id IS NULL AS missing_clean_domains,
  i.stay_id IS NULL AS missing_clean_flags
FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` f
LEFT JOIN `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean` c
  USING (subject_id, hadm_id, stay_id, day_idx)
LEFT JOIN `strange-math-456415-c3.mimic_analysis.improvement_flags_clean` i
  USING (subject_id, hadm_id, stay_id, day_idx)
WHERE c.stay_id IS NULL
   OR i.stay_id IS NULL
ORDER BY f.stay_id, f.day_idx
LIMIT 500;

