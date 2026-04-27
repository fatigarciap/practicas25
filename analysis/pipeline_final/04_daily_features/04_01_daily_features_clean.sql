CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.daily_features_clean` AS

WITH base AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    window_start,
    window_end
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
),

vitals_raw AS (
  SELECT
    ce.subject_id,
    ce.hadm_id,
    ce.stay_id,
    CAST(ce.charttime AS TIMESTAMP) AS charttime_ts,
    CASE
      WHEN ce.itemid IN (220045, 211) THEN 'HR'
      WHEN ce.itemid IN (220181, 456) THEN 'MAP'
      WHEN ce.itemid IN (220179, 678) THEN 'SysBP'
      WHEN ce.itemid IN (220180, 679) THEN 'DiasBP'
      WHEN ce.itemid IN (223761, 223762, 676) THEN 'Temp'
      WHEN ce.itemid IN (220210, 683) THEN 'RR'
      WHEN ce.itemid IN (220277, 646) THEN 'SpO2'
      WHEN ce.itemid = 223835 THEN 'FiO2'
      ELSE NULL
    END AS variable,
    CASE
      WHEN ce.itemid = 223761 THEN
        CASE
          WHEN (ce.valuenum - 32) * 5/9 BETWEEN 25 AND 45
          THEN ROUND((ce.valuenum - 32) * 5/9, 2)
          ELSE NULL
        END
      WHEN ce.itemid IN (223762, 676) THEN
        CASE
          WHEN ce.valuenum BETWEEN 25 AND 45 THEN ROUND(ce.valuenum, 2)
          ELSE NULL
        END
      WHEN ce.itemid IN (220045,211) THEN
        CASE WHEN ce.valuenum BETWEEN 20 AND 250 THEN ce.valuenum ELSE NULL END
      WHEN ce.itemid IN (220181,456) THEN
        CASE WHEN ce.valuenum BETWEEN 20 AND 200 THEN ce.valuenum ELSE NULL END
      WHEN ce.itemid IN (220179,678) THEN
        CASE WHEN ce.valuenum BETWEEN 30 AND 300 THEN ce.valuenum ELSE NULL END
      WHEN ce.itemid IN (220180,679) THEN
        CASE WHEN ce.valuenum BETWEEN 10 AND 200 THEN ce.valuenum ELSE NULL END
      WHEN ce.itemid IN (220210,683) THEN
        CASE WHEN ce.valuenum BETWEEN 5 AND 80 THEN ce.valuenum ELSE NULL END
      WHEN ce.itemid IN (220277,646) THEN
        CASE
          WHEN ce.valuenum BETWEEN 0 AND 1 THEN ce.valuenum * 100
          WHEN ce.valuenum BETWEEN 50 AND 100 THEN ce.valuenum
          ELSE NULL
        END
      WHEN ce.itemid = 223835 THEN
        CASE
          WHEN ce.valuenum BETWEEN 0.21 AND 1 THEN ce.valuenum
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

labs_raw AS (
  SELECT
    le.subject_id,
    le.hadm_id,
    CAST(le.charttime AS TIMESTAMP) AS charttime_ts,
    CASE
      WHEN le.itemid IN (51301, 51516, 51300) THEN 'WBC'
      WHEN le.itemid = 50813 THEN 'Lactate'
      WHEN le.itemid = 50912 THEN 'Creatinine'
      WHEN le.itemid = 50885 THEN 'Bilirubin'
      WHEN le.itemid = 51265 THEN 'Platelets'
      WHEN le.itemid = 51222 THEN 'Hemoglobin'
      ELSE NULL
    END AS variable,
    CASE
      WHEN le.itemid IN (51301, 51516, 51300) THEN
        CASE WHEN le.valuenum BETWEEN 0.1 AND 400 THEN le.valuenum ELSE NULL END
      WHEN le.itemid = 50813 THEN
        CASE WHEN le.valuenum BETWEEN 0.1 AND 30 THEN le.valuenum ELSE NULL END
      WHEN le.itemid = 50912 THEN
        CASE WHEN le.valuenum BETWEEN 0.1 AND 20 THEN le.valuenum ELSE NULL END
      WHEN le.itemid = 50885 THEN
        CASE WHEN le.valuenum BETWEEN 0.1 AND 60 THEN le.valuenum ELSE NULL END
      WHEN le.itemid = 51265 THEN
        CASE WHEN le.valuenum BETWEEN 1 AND 2000 THEN le.valuenum ELSE NULL END
      WHEN le.itemid = 51222 THEN
        CASE WHEN le.valuenum BETWEEN 1 AND 25 THEN le.valuenum ELSE NULL END
      ELSE NULL
    END AS valuenum
  FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
  WHERE le.itemid IN (51301, 51516, 51300, 50813, 50912, 50885, 51265, 51222)
    AND le.valuenum IS NOT NULL
    AND le.charttime IS NOT NULL
),

combined AS (
  SELECT
    b.hadm_id,
    b.stay_id,
    b.day_idx,
    v.variable,
    v.valuenum
  FROM base b
  JOIN vitals_raw v
    ON v.stay_id = b.stay_id
   AND v.charttime_ts >= b.window_start
   AND v.charttime_ts < b.window_end
  WHERE v.variable IS NOT NULL
    AND v.valuenum IS NOT NULL

  UNION ALL

  SELECT
    b.hadm_id,
    b.stay_id,
    b.day_idx,
    l.variable,
    l.valuenum
  FROM base b
  JOIN labs_raw l
    ON l.hadm_id = b.hadm_id
   AND l.charttime_ts >= b.window_start
   AND l.charttime_ts < b.window_end
  WHERE l.variable IS NOT NULL
    AND l.valuenum IS NOT NULL
),

agg AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    variable,
    COUNT(*) AS n_obs,
    APPROX_QUANTILES(valuenum, 100)[OFFSET(50)] AS median_value
  FROM combined
  GROUP BY hadm_id, stay_id, day_idx, variable
),

pivoted AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    MAX(IF(variable = 'HR', median_value, NULL)) AS HR_median,
    MAX(IF(variable = 'MAP', median_value, NULL)) AS MAP_median,
    MAX(IF(variable = 'SysBP', median_value, NULL)) AS SysBP_median,
    MAX(IF(variable = 'DiasBP', median_value, NULL)) AS DiasBP_median,
    MAX(IF(variable = 'Temp', median_value, NULL)) AS Temp_median,
    MAX(IF(variable = 'RR', median_value, NULL)) AS RR_median,
    MAX(IF(variable = 'SpO2', median_value, NULL)) AS SpO2_median,
    MAX(IF(variable = 'FiO2', median_value, NULL)) AS FiO2_median,
    MAX(IF(variable = 'WBC', median_value, NULL)) AS WBC_median,
    MAX(IF(variable = 'Lactate', median_value, NULL)) AS Lactate_median,
    MAX(IF(variable = 'Creatinine', median_value, NULL)) AS Creatinine_median,
    MAX(IF(variable = 'Bilirubin', median_value, NULL)) AS Bilirubin_median,
    MAX(IF(variable = 'Platelets', median_value, NULL)) AS Platelets_median,
    MAX(IF(variable = 'Hemoglobin', median_value, NULL)) AS Hgb_median
  FROM agg
  GROUP BY hadm_id, stay_id, day_idx
)

SELECT
  b.subject_id,
  b.hadm_id,
  b.stay_id,
  b.day_idx,
  b.window_start,
  b.window_end,

  p.HR_median,
  p.MAP_median,
  p.SysBP_median,
  p.DiasBP_median,
  p.Temp_median,
  p.RR_median,
  p.SpO2_median,
  p.FiO2_median,
  p.WBC_median,
  p.Lactate_median,
  p.Creatinine_median,
  p.Bilirubin_median,
  p.Platelets_median,
  p.Hgb_median,

  CASE
    WHEN p.FiO2_median IS NULL OR p.FiO2_median = 0 THEN NULL
    WHEN p.SpO2_median IS NULL THEN NULL
    ELSE p.SpO2_median / p.FiO2_median
  END AS spo2fio2_ratio

FROM base b
LEFT JOIN pivoted p
USING (hadm_id, stay_id, day_idx);
