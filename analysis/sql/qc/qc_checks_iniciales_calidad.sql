-- qc_checks_iniciales_calidad.sql
-- BigQuery Standard SQL
--
-- Devuelve una tabla resumida con los checks iniciales de calidad de la
-- tabla longitudinal final del proyecto amr_days/MIMIC-IV.
--
-- Si tu dataset cambia, reemplaza:
--   `strange-math-456415-c3.mimic_analysis`
-- por:
--   `PROJECT_ID.DATASET_ID`

WITH longitudinal AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
),

duplicated_stay_day AS (
  SELECT
    stay_id,
    day_idx,
    COUNT(*) AS n_rows
  FROM longitudinal
  GROUP BY stay_id, day_idx
  HAVING COUNT(*) > 1
),

per_stay AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    ANY_VALUE(t0) AS t0,
    ANY_VALUE(icu_intime) AS icu_intime,
    ANY_VALUE(icu_outtime) AS icu_outtime,
    ANY_VALUE(deathtime) AS deathtime,
    ANY_VALUE(followup_end) AS followup_end,
    MIN(day_idx) AS min_day_idx,
    MAX(day_idx) AS max_day_idx,
    COUNT(*) AS n_rows,
    MAX(COALESCE(sustained_improvement, 0)) AS ever_sustained_improvement
  FROM longitudinal
  GROUP BY subject_id, hadm_id, stay_id
),

first_sustained AS (
  SELECT
    stay_id,
    MIN(day_idx) AS first_sustained_day
  FROM longitudinal
  WHERE sustained_improvement = 1
  GROUP BY stay_id
),

metrics AS (
  -- Recuento general
  SELECT
    1 AS order_id,
    'Recuento general' AS section,
    'Filas tabla longitudinal' AS metric,
    CAST(COUNT(*) AS STRING) AS value
  FROM longitudinal

  UNION ALL
  SELECT
    2,
    'Recuento general',
    'Estancias UCI',
    CAST(COUNT(DISTINCT stay_id) AS STRING)
  FROM longitudinal

  UNION ALL
  SELECT
    3,
    'Recuento general',
    'Pacientes',
    CAST(COUNT(DISTINCT subject_id) AS STRING)
  FROM longitudinal

  UNION ALL
  SELECT
    4,
    'Recuento general',
    'Dia minimo',
    CAST(MIN(day_idx) AS STRING)
  FROM longitudinal

  UNION ALL
  SELECT
    5,
    'Recuento general',
    'Dia maximo',
    CAST(MAX(day_idx) AS STRING)
  FROM longitudinal

  -- Coherencia temporal
  UNION ALL
  SELECT
    6,
    'Coherencia temporal',
    'Ventanas con window_start >= window_end',
    CAST(COUNTIF(window_start >= window_end) AS STRING)
  FROM longitudinal

  UNION ALL
  SELECT
    7,
    'Coherencia temporal',
    'T0 fuera de estancia UCI',
    CAST(COUNT(DISTINCT IF(t0 < icu_intime OR t0 > icu_outtime, stay_id, NULL)) AS STRING)
  FROM longitudinal

  UNION ALL
  SELECT
    8,
    'Coherencia temporal',
    'Filas duplicadas por stay_id + day_idx',
    CAST(IFNULL(SUM(n_rows - 1), 0) AS STRING)
  FROM duplicated_stay_day

  -- Outcome
  UNION ALL
  SELECT
    9,
    'Outcome',
    'Pacientes con sustained_improvement',
    CAST(COUNT(DISTINCT IF(ever_sustained_improvement = 1, stay_id, NULL)) AS STRING)
  FROM per_stay

  UNION ALL
  SELECT
    10,
    'Outcome',
    'Pacientes sin sustained_improvement',
    CAST(COUNT(DISTINCT IF(ever_sustained_improvement = 0, stay_id, NULL)) AS STRING)
  FROM per_stay

  UNION ALL
  SELECT
    11,
    'Outcome',
    'Primer dia mediano de mejoria sostenida',
    COALESCE(
      CAST(APPROX_QUANTILES(first_sustained_day, 100)[SAFE_OFFSET(50)] AS STRING),
      'pendiente'
    )
  FROM first_sustained

  -- Seguimiento
  UNION ALL
  SELECT
    12,
    'Seguimiento',
    'Pacientes truncados por muerte',
    CAST(COUNT(DISTINCT IF(deathtime IS NOT NULL AND followup_end = deathtime, stay_id, NULL)) AS STRING)
  FROM per_stay

  UNION ALL
  SELECT
    13,
    'Seguimiento',
    'Pacientes truncados por alta UCI',
    CAST(COUNT(DISTINCT IF(followup_end = icu_outtime, stay_id, NULL)) AS STRING)
  FROM per_stay

  UNION ALL
  SELECT
    14,
    'Seguimiento',
    'Pacientes que llegan a dia 30',
    CAST(COUNT(DISTINCT IF(followup_end = TIMESTAMP_ADD(t0, INTERVAL 30 DAY), stay_id, NULL)) AS STRING)
  FROM per_stay
)

SELECT
  section,
  metric,
  value
FROM metrics
ORDER BY order_id;
