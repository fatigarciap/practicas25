-- ============================================
-- 🧩 BLOQUE 2: DAILY FEATURES (VITALES + LABS)
-- Unidad: stay_id – day_idx (una fila por día)
-- ============================================

CREATE OR REPLACE TABLE
  `strange-math-456415-c3.mimic_analysis.daily_features` AS

WITH
-- 1️⃣ Ventanas temporales (output limpio del Bloque 1)
base AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    window_start,
    window_end
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
),

-- 2️⃣ VITALES RAW (normalizados)
vitals_raw AS (
  SELECT
    ce.subject_id,
    ce.hadm_id,
    ce.stay_id,
    CAST(ce.charttime AS TIMESTAMP) AS charttime_ts,
    CASE
      WHEN itemid IN (220045, 211) THEN 'HR'
      WHEN itemid IN (220181, 456) THEN 'MAP'
      WHEN itemid IN (220179, 678) THEN 'SysBP'
      WHEN itemid IN (220180, 679) THEN 'DiasBP'
      WHEN itemid IN (223761, 223762, 676) THEN 'Temp'
      WHEN itemid IN (220210, 683) THEN 'RR'
      WHEN itemid IN (220277, 646) THEN 'SpO2'
      WHEN itemid = 223835 THEN 'FiO2'
      ELSE NULL
    END AS variable,

    CASE
      -- Temp F → C
      WHEN itemid = 223761 THEN
        CASE
          WHEN (ce.valuenum - 32) * 5/9 BETWEEN 25 AND 45
          THEN ROUND((ce.valuenum - 32) * 5/9, 2)
          ELSE NULL
        END

      -- Temp C
      WHEN itemid = 223762 THEN
        CASE
          WHEN ce.valuenum BETWEEN 25 AND 45
          THEN ROUND(ce.valuenum, 2)
          ELSE NULL
        END

      -- HR / BP
      WHEN itemid IN (220045,211,220181,456,220179,678,220180,679) THEN
        CASE WHEN ce.valuenum BETWEEN 30 AND 250 THEN ce.valuenum ELSE NULL END

      -- RR
      WHEN itemid IN (220210,683) THEN
        CASE WHEN ce.valuenum BETWEEN 5 AND 60 THEN ce.valuenum ELSE NULL END

      -- SpO2
      WHEN itemid IN (220277,646) THEN
        CASE
          WHEN ce.valuenum BETWEEN 0 AND 1 THEN ce.valuenum * 100
          WHEN ce.valuenum BETWEEN 50 AND 100 THEN ce.valuenum
          ELSE NULL
        END

      -- FiO2
      WHEN itemid = 223835 THEN
        CASE
          WHEN ce.valuenum BETWEEN 0 AND 1 THEN ce.valuenum
          WHEN ce.valuenum BETWEEN 21 AND 100 THEN ce.valuenum / 100
          ELSE NULL
        END

      ELSE NULL
    END AS valuenum
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
  WHERE ce.itemid IN (
    211,220045,456,220181,678,220179,679,220180,
    223761,223762,676,220210,683,220277,646,223835
  )
    AND ce.valuenum IS NOT NULL
    AND ce.charttime IS NOT NULL
),

-- 3️⃣ LABS RAW
labs_raw AS (
  SELECT
    le.subject_id,
    le.hadm_id,
    CAST(le.charttime AS TIMESTAMP) AS charttime_ts,
    CASE
      WHEN itemid = 50868 THEN 'WBC'
      WHEN itemid = 50862 THEN 'Lactate'
      WHEN itemid = 50912 THEN 'Creatinine'
      WHEN itemid = 50882 THEN 'Bilirubin'
      WHEN itemid = 51265 THEN 'Platelets'
      WHEN itemid = 50820 THEN 'CRP'
      WHEN itemid = 51222 THEN 'Hemoglobin'
      ELSE NULL
    END AS variable,
    le.valuenum
  FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
  WHERE le.itemid IN (50868,50862,50912,50882,51265,50820,51222)
    AND le.valuenum IS NOT NULL
    AND le.charttime IS NOT NULL
),

-- 4️⃣ Asociar eventos a ventanas (TIMESTAMP ↔ TIMESTAMP)
combined AS (
  -- VITALES
  SELECT
    b.hadm_id,
    b.stay_id,
    b.day_idx,
    v.variable,
    v.valuenum
  FROM vitals_raw v
  JOIN base b
    ON v.stay_id = b.stay_id
   AND v.charttime_ts >= b.window_start
   AND v.charttime_ts <  b.window_end

  UNION ALL

  -- LABS
  SELECT
    b.hadm_id,
    b.stay_id,
    b.day_idx,
    l.variable,
    l.valuenum
  FROM labs_raw l
  JOIN base b
    ON l.hadm_id = b.hadm_id
   AND l.charttime_ts >= b.window_start
   AND l.charttime_ts <  b.window_end
),

-- 5️⃣ Agregación diaria
agg AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    variable,
    COUNT(*) AS n_obs,
    APPROX_QUANTILES(valuenum, 100)[OFFSET(50)] AS median_value
  FROM combined
  WHERE valuenum IS NOT NULL
  GROUP BY hadm_id, stay_id, day_idx, variable
),

-- 6️⃣ Pivot por día
pivoted AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    MAX(IF(variable='HR', median_value, NULL)) AS HR_median,
    MAX(IF(variable='MAP', median_value, NULL)) AS MAP_median,
    MAX(IF(variable='Temp', median_value, NULL)) AS Temp_median,
    MAX(IF(variable='RR', median_value, NULL)) AS RR_median,
    MAX(IF(variable='SpO2', median_value, NULL)) AS SpO2_median,
    MAX(IF(variable='FiO2', median_value, NULL)) AS FiO2_median,
    MAX(IF(variable='WBC', median_value, NULL)) AS WBC_median,
    MAX(IF(variable='Lactate', median_value, NULL)) AS Lactate_median,
    MAX(IF(variable='Creatinine', median_value, NULL)) AS Creatinine_median,
    MAX(IF(variable='Bilirubin', median_value, NULL)) AS Bilirubin_median,
    MAX(IF(variable='Platelets', median_value, NULL)) AS Platelets_median,
    MAX(IF(variable='CRP', median_value, NULL)) AS CRP_median,
    MAX(IF(variable='Hemoglobin', median_value, NULL)) AS Hgb_median
  FROM agg
  GROUP BY hadm_id, stay_id, day_idx
)

-- 7️⃣ SELECT FINAL + S/F ratio
SELECT
  *,
  CASE
    WHEN FiO2_median IS NULL OR FiO2_median = 0 THEN NULL
    WHEN SpO2_median IS NULL THEN NULL
    ELSE SpO2_median / FiO2_median
  END AS spo2fio2_ratio
FROM pivoted
ORDER BY stay_id, day_idx;