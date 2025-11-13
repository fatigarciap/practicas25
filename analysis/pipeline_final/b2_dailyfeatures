-- ============================================
-- 🧩 BLOQUE 2: DAILY FEATURES (VITALES + LABS) - CORREGIDO
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.daily_features` AS
WITH

-- 1️⃣ Base temporal (ventanas de 24h del Bloque 1)
base AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
),

-- 2️⃣ VITALES RAW (Temperatura en Celsius; SpO2 en %, FiO2 en fracción 0.21-1.0)
vitals_raw AS (
  SELECT
    ce.subject_id,
    ce.hadm_id,
    ce.stay_id,
    ce.charttime,
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
    -- Normalización defensiva de las unidades:
    CASE
      -- Temperatura °F a °C (itemid 223761)
      WHEN itemid = 223761 THEN
        CASE WHEN (SAFE_CAST(ce.valuenum AS FLOAT64) - 32) * 5/9 BETWEEN 25 AND 45
             THEN ROUND((SAFE_CAST(ce.valuenum AS FLOAT64) - 32) * 5/9, 2)
             ELSE NULL END

      -- Temperatura °C (itemid 223762)
      WHEN itemid = 223762 THEN
        CASE WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 25 AND 45
             THEN ROUND(SAFE_CAST(ce.valuenum AS FLOAT64), 2)
             ELSE NULL END

      -- Rangos generales para otras variables (presiones, FC, etc.)
      WHEN itemid IN (220045, 211, 220181, 456, 220179, 678, 220180, 679) THEN
        CASE WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 30 AND 250
             THEN SAFE_CAST(ce.valuenum AS FLOAT64)
             ELSE NULL END

      WHEN itemid IN (220210, 683) THEN
        CASE WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 5 AND 60
             THEN SAFE_CAST(ce.valuenum AS FLOAT64)
             ELSE NULL END

      -- SpO2: aseguramos que quede en porcentaje 0-100
      WHEN itemid IN (220277, 646) THEN
        CASE
          WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 0.0 AND 1.0
            THEN -- estaba en fracción (p.ej. 0.95) -> convertir a %
              SAFE_CAST(ce.valuenum AS FLOAT64) * 100.0
          WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 50 AND 100
            THEN SAFE_CAST(ce.valuenum AS FLOAT64) -- ya en %
          ELSE NULL
        END

      -- FiO2: normalizamos a fracción 0.21-1.0
      WHEN itemid = 223835 THEN
        CASE
          WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 0.0 AND 1.0
            THEN SAFE_CAST(ce.valuenum AS FLOAT64) -- ya en fracción
          WHEN SAFE_CAST(ce.valuenum AS FLOAT64) BETWEEN 21 AND 100
            THEN SAFE_CAST(ce.valuenum AS FLOAT64) / 100.0 -- porcentaje -> fracción
          ELSE NULL
        END

      -- Por defecto, devolver valor numérico (labs u otros)
      ELSE SAFE_CAST(ce.valuenum AS FLOAT64)
    END AS valuenum
  FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
  WHERE itemid IN (
    211,220045,456,220181,678,220179,679,220180,
    223761,223762,676,220210,683,220277,646,223835
  )
    AND ce.valuenum IS NOT NULL
),

-- 3️⃣ LABS RAW
labs_raw AS (
  SELECT
    le.subject_id,
    le.hadm_id,
    le.charttime,
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
    SAFE_CAST(le.valuenum AS FLOAT64) AS valuenum
  FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
  WHERE itemid IN (50868,50862,50912,50882,51265,50820,51222)
    AND le.valuenum IS NOT NULL
),

-- 4️⃣ UNIR VITALES + LABS CON VENTANAS
combined AS (
  SELECT b.subject_id, b.hadm_id, b.stay_id, b.day_idx, v.variable, v.valuenum
  FROM vitals_raw v
  JOIN base b
    ON v.subject_id = b.subject_id
   AND v.stay_id = b.stay_id
   AND TIMESTAMP(v.charttime) BETWEEN b.window_start AND b.window_end

  UNION ALL

  SELECT b.subject_id, b.hadm_id, b.stay_id, b.day_idx, l.variable, l.valuenum
  FROM labs_raw l
  JOIN base b
    ON l.subject_id = b.subject_id
   AND l.hadm_id = b.hadm_id
   AND TIMESTAMP(l.charttime) BETWEEN b.window_start AND b.window_end
),

-- 5️⃣ AGREGAR estadísticos por paciente, día y variable (mediana entre otros)
agg AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    variable,
    COUNT(*) AS n_obs,
    APPROX_QUANTILES(valuenum, 100)[OFFSET(50)] AS median_value,
    MIN(valuenum) AS min_value,
    MAX(valuenum) AS max_value,
    AVG(valuenum) AS mean_value
  FROM combined
  WHERE valuenum IS NOT NULL
  GROUP BY subject_id, hadm_id, stay_id, day_idx, variable
),

-- 6️⃣ PIVOT INICIAL (medianas por variable)
pivoted AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    MAX(CASE WHEN variable='HR' THEN median_value END) AS HR_median,
    MAX(CASE WHEN variable='MAP' THEN median_value END) AS MAP_median,
    MAX(CASE WHEN variable='Temp' THEN median_value END) AS Temp_median,
    MAX(CASE WHEN variable='RR' THEN median_value END) AS RR_median,
    MAX(CASE WHEN variable='SpO2' THEN median_value END) AS SpO2_median_raw, -- en %
    MAX(CASE WHEN variable='FiO2' THEN median_value END) AS FiO2_median_raw, -- en fracción
    MAX(CASE WHEN variable='WBC' THEN median_value END) AS WBC_median,
    MAX(CASE WHEN variable='Lactate' THEN median_value END) AS Lactate_median,
    MAX(CASE WHEN variable='Creatinine' THEN median_value END) AS Creatinine_median,
    MAX(CASE WHEN variable='Bilirubin' THEN median_value END) AS Bilirubin_median,
    MAX(CASE WHEN variable='Platelets' THEN median_value END) AS Platelets_median,
    MAX(CASE WHEN variable='CRP' THEN median_value END) AS CRP_median,
    MAX(CASE WHEN variable='Hemoglobin' THEN median_value END) AS Hgb_median
  FROM agg
  GROUP BY subject_id, hadm_id, stay_id, day_idx
),

-- 7️⃣ NORMALIZACIONES FINALES y CÁLCULO del S/F ratio (SpO2_pct / FiO2_frac)
normalized AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    HR_median,
    MAP_median,
    Temp_median,
    RR_median,

    -- SpO2 ya debe venir en %, pero protegemos nulos
    CASE
      WHEN SpO2_median_raw IS NULL THEN NULL
      WHEN SpO2_median_raw BETWEEN 0 AND 1 THEN SpO2_median_raw * 100.0
      ELSE SpO2_median_raw
    END AS SpO2_pct,

    -- FiO2 ya debe venir como fracción; garantizamos rango 0.0-1.0
    CASE
      WHEN FiO2_median_raw IS NULL THEN NULL
      WHEN FiO2_median_raw > 1.0 AND FiO2_median_raw <= 100.0 THEN FiO2_median_raw / 100.0
      ELSE FiO2_median_raw
    END AS FiO2_frac,

    WBC_median,
    Lactate_median,
    Creatinine_median,
    Bilirubin_median,
    Platelets_median,
    CRP_median,
    Hgb_median
  FROM pivoted
),

-- 8️⃣ (Opcional) resumen QC en la misma consulta para inspección rápida
qc_summary AS (
  SELECT
    COUNTIF(FiO2_frac IS NULL) AS n_FiO2_null,
    COUNTIF(FiO2_frac IS NOT NULL AND FiO2_frac BETWEEN 0.0 AND 0.2) AS n_FiO2_low_le_0_2,
    COUNTIF(FiO2_frac IS NOT NULL AND FiO2_frac BETWEEN 0.21 AND 1.0) AS n_FiO2_norm_0_21_1,
    COUNTIF(SpO2_pct IS NULL) AS n_SpO2_null,
    COUNTIF(SpO2_pct IS NOT NULL AND SpO2_pct BETWEEN 50 AND 100) AS n_SpO2_norm_50_100
  FROM normalized
)

-- 9️⃣ SELECT final: variables + spo2fio2_ratio
SELECT
  n.subject_id,
  n.hadm_id,
  n.stay_id,
  n.day_idx,
  n.HR_median,
  n.MAP_median,
  n.Temp_median,
  n.RR_median,
  n.SpO2_pct        AS SpO2_median,
  n.FiO2_frac       AS FiO2_median,

  -- S/F ratio seguro: SpO2 (%) dividido por FiO2 (fracción), protegiendo división por 0/NULL
  CASE
    WHEN n.FiO2_frac IS NULL OR n.FiO2_frac = 0 THEN NULL
    WHEN n.SpO2_pct IS NULL THEN NULL
    ELSE n.SpO2_pct / n.FiO2_frac
  END AS spo2fio2_ratio,

  n.WBC_median,
  n.Lactate_median,
  n.Creatinine_median,
  n.Bilirubin_median,
  n.Platelets_median,
  n.CRP_median,
  n.Hgb_median

FROM normalized n
ORDER BY n.subject_id, n.stay_id, n.day_idx;
