CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.base_windows` AS
WITH cohort AS (
  -- Cohorte a nivel estancia (tu tabla actual)
  SELECT
    stay_id,
    hadm_id,
    index_charttime
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_stay`
),

icu_context AS (
  -- Contexto UCI + muerte
  SELECT
    c.stay_id,
    c.hadm_id,
    c.index_charttime,
    i.intime,
    i.outtime,
    a.deathtime
  FROM cohort c
  JOIN `physionet-data.mimiciv_3_1_icu.icustays` i
    ON c.stay_id = i.stay_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` a
    ON c.hadm_id = a.hadm_id
),

abx_start AS (
  -- 1) Primer inicio de antibiótico dentro de la estancia UCI
  -- 2) (Opcional) lo acotamos a +/- 48h del index_charttime para que sea coherente con "evento índice"
  SELECT
    i.stay_id,
    i.hadm_id,
    MIN(CAST(p.starttime AS TIMESTAMP)) AS t0
  FROM icu_context i
  JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON i.hadm_id = p.hadm_id
  WHERE
    p.starttime IS NOT NULL
    -- antibióticos: usamos LIKE para no perder por nombres no exactos
    AND (
      LOWER(p.drug) LIKE '%ceftriaxone%' OR
      LOWER(p.drug) LIKE '%cefepime%' OR
      LOWER(p.drug) LIKE '%ceftazidime%' OR
      LOWER(p.drug) LIKE '%meropenem%' OR
      LOWER(p.drug) LIKE '%imipenem%' OR
      LOWER(p.drug) LIKE '%trimethoprim%' OR
      LOWER(p.drug) LIKE '%sulfamethoxazole%' OR
      LOWER(p.drug) LIKE '%linezolid%' OR
      LOWER(p.drug) LIKE '%daptomycin%' OR
      LOWER(p.drug) LIKE '%oxacillin%' OR
      LOWER(p.drug) LIKE '%vancomycin%'
    )
    -- t0 debe caer durante la UCI
    AND CAST(p.starttime AS TIMESTAMP) BETWEEN CAST(i.intime AS TIMESTAMP) AND CAST(i.outtime AS TIMESTAMP)
    -- acotar alrededor del evento índice (si quieres quitar esto, borra estas 2 líneas)
    AND CAST(p.starttime AS TIMESTAMP) BETWEEN
      TIMESTAMP_SUB(CAST(i.index_charttime AS TIMESTAMP), INTERVAL 48 HOUR)
      AND
      TIMESTAMP_ADD(CAST(i.index_charttime AS TIMESTAMP), INTERVAL 48 HOUR)
  GROUP BY i.stay_id, i.hadm_id
),

icu_with_t0 AS (
  -- Nos quedamos solo con stays donde hemos encontrado inicio de antibiótico
  SELECT
    i.stay_id,
    i.hadm_id,
    a.t0,
    CAST(i.intime AS TIMESTAMP) AS intime,
    CAST(i.outtime AS TIMESTAMP) AS outtime,
    CAST(i.deathtime AS TIMESTAMP) AS deathtime
  FROM icu_context i
  JOIN abx_start a
    USING (stay_id, hadm_id)
),

followup AS (
  -- Fin del seguimiento: alta UCI / muerte / 30 días desde t0
  SELECT
    stay_id,
    hadm_id,
    t0,
    LEAST(
      outtime,
      IFNULL(deathtime, outtime),
      TIMESTAMP_ADD(t0, INTERVAL 30 DAY)
    ) AS followup_end
  FROM icu_with_t0
),

expanded_days AS (
  -- Índices diarios desde t0
  SELECT
    stay_id,
    hadm_id,
    t0,
    followup_end,
    GENERATE_ARRAY(
      0,
      CAST(TIMESTAMP_DIFF(followup_end, t0, HOUR) / 24 AS INT64)
    ) AS day_indices
  FROM followup
),

windows AS (
  -- Ventanas de 24h
  SELECT
    stay_id,
    hadm_id,
    t0,
    followup_end,
    day_idx,
    TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) AS window_start,
    TIMESTAMP_ADD(t0, INTERVAL (day_idx + 1) * 24 HOUR) AS window_end
  FROM expanded_days, UNNEST(day_indices) AS day_idx
  WHERE TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) < followup_end
)

SELECT
  stay_id,
  hadm_id,
  t0,
  day_idx,
  window_start,
  window_end,
  followup_end
FROM windows
ORDER BY stay_id, day_idx;