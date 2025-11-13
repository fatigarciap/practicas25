-- ============================================
-- 🧩 BLOQUE 1: BASE Y VENTANAS DE 24h
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.base_windows` AS
WITH
-- ------------------------------------------------
-- 1️⃣ Identificar inicio de antibiótico (T₀)
-- ------------------------------------------------
antibiotic_start AS (
  SELECT
    p.subject_id,
    p.hadm_id,
    MIN(p.starttime) AS abx_starttime
  FROM `physionet-data.mimiciv_3_1_hosp.prescriptions` p
  WHERE LOWER(p.drug) LIKE '%cef%'    -- cefalosporinas
     OR LOWER(p.drug) LIKE '%piper%'  -- piperacilina/tazobactam
     OR LOWER(p.drug) LIKE '%mero%'   -- meropenem
     OR LOWER(p.drug) LIKE '%van%'    -- vancomicina
     OR LOWER(p.drug) LIKE '%amox%'   -- amoxicilina
  GROUP BY p.subject_id, p.hadm_id
),

-- ------------------------------------------------
-- 2️⃣ Contexto de estancia en UCI + mortalidad
-- ------------------------------------------------
icu_context AS (
  SELECT
    icu.subject_id,
    icu.hadm_id,
    icu.stay_id,
    icu.intime,
    icu.outtime,
    adm.deathtime
  FROM `physionet-data.mimiciv_3_1_icu.icustays` icu
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` adm
    USING (hadm_id)
),

-- ------------------------------------------------
-- 3️⃣ Unir antibióticos con estancia en UCI
-- ------------------------------------------------
joined AS (
  SELECT
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    CAST(i.intime AS TIMESTAMP) AS icu_in,
    CAST(i.outtime AS TIMESTAMP) AS icu_out,
    CAST(a.abx_starttime AS TIMESTAMP) AS abx_starttime,
    CAST(i.deathtime AS TIMESTAMP) AS death_time
  FROM icu_context i
  JOIN antibiotic_start a
    USING (subject_id, hadm_id)
  WHERE a.abx_starttime BETWEEN i.intime AND i.outtime
),

-- ------------------------------------------------
-- 4️⃣ Definir rango de seguimiento (máx 30 días)
-- ------------------------------------------------
window_limits AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    abx_starttime AS t0,
    LEAST(icu_out, TIMESTAMP_ADD(abx_starttime, INTERVAL 30 DAY)) AS followup_end,  -- máx 30 días o alta
    TIMESTAMP_DIFF(LEAST(icu_out, TIMESTAMP_ADD(abx_starttime, INTERVAL 30 DAY)), abx_starttime, HOUR) AS total_hours
  FROM joined
),

-- ------------------------------------------------
-- 5️⃣ Expandir ventanas de 24h desde T₀
-- ------------------------------------------------
window_index AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    t0,
    followup_end,
    total_hours,
    GENERATE_ARRAY(0, CAST(total_hours / 24 AS INT64)) AS day_indices
  FROM window_limits
),

windows AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    t0,
    followup_end,
    day_idx,
    TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) AS window_start,
    TIMESTAMP_ADD(t0, INTERVAL (day_idx + 1) * 24 HOUR) AS window_end
  FROM window_index,
       UNNEST(day_indices) AS day_idx
  WHERE TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) < followup_end
)

-- ------------------------------------------------
-- 🧾 Tabla base: una fila por paciente y ventana de 24h
-- ------------------------------------------------
SELECT
  subject_id,
  hadm_id,
  stay_id,
  t0,
  day_idx,
  window_start,
  window_end,
  followup_end
FROM windows
ORDER BY subject_id, stay_id, day_idx;
