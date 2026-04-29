-- proposito:
--   Evaluar consistencia temporal entre indice microbiologico, t0 antibiotico,
--   estancia UCI, ventanas diarias y seguimiento.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
--   - `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
--   - `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean`
--   - `strange-math-456415-c3.mimic_analysis.new_foci_events_clean`
--
-- nivel de agregacion:
--   - Una fila por regla temporal auditada.
--
-- claves esperadas:
--   - subject_id, hadm_id, stay_id, microevent_id para t0.
--   - stay_id, day_idx para ventanas.
--   - stay_id, hadm_id, charttime para eventos posteriores.
--
-- posibles errores detectables:
--   - t0 fuera de UCI.
--   - t0 fuera de la ventana +/- 48 h respecto al cultivo indice.
--   - ventanas con inicio posterior o igual al fin.
--   - ventanas fuera del seguimiento.
--   - eventos de radiologia o nuevo foco antes de t0.

WITH temporal_checks AS (
  SELECT
    't0_before_icu_intime' AS check_name,
    COUNTIF(true_t0 < icu_intime) AS n_violations,
    COUNT(*) AS n_rows
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT
    't0_after_icu_outtime',
    COUNTIF(true_t0 > icu_outtime),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT
    't0_more_than_48h_before_index',
    COUNTIF(true_t0 < TIMESTAMP_SUB(index_charttime, INTERVAL 48 HOUR)),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT
    't0_more_than_48h_after_index',
    COUNTIF(true_t0 > TIMESTAMP_ADD(index_charttime, INTERVAL 48 HOUR)),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT
    'window_start_not_before_window_end',
    COUNTIF(window_start >= window_end),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`

  UNION ALL
  SELECT
    'window_start_before_t0',
    COUNTIF(window_start < t0),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`

  UNION ALL
  SELECT
    'window_end_after_followup_end',
    COUNTIF(window_end > followup_end),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`

  UNION ALL
  SELECT
    'radiology_event_not_after_t0',
    COUNTIF(r.charttime <= w.t0),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean` r
  JOIN (
    SELECT DISTINCT stay_id, hadm_id, t0
    FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
  ) w
  USING (stay_id, hadm_id)

  UNION ALL
  SELECT
    'new_foci_event_not_after_t0',
    COUNTIF(n.charttime <= w.t0),
    COUNT(*)
  FROM `strange-math-456415-c3.mimic_analysis.new_foci_events_clean` n
  JOIN (
    SELECT DISTINCT stay_id, hadm_id, t0
    FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
  ) w
  USING (stay_id, hadm_id)
)

SELECT
  check_name,
  n_violations,
  n_rows,
  SAFE_DIVIDE(n_violations, n_rows) AS violation_fraction
FROM temporal_checks
ORDER BY check_name;
