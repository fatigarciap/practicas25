CREATE OR REPLACE TABLE
  `strange-math-456415-c3.mimic_analysis.clinical_domains_sci` AS

WITH
daily AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.daily_features`
),

new_foci AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.new_foci_flag`
)

SELECT
  d.hadm_id,
  d.stay_id,
  d.day_idx,

  -- NO-EVENTO: nuevos focos
  nf.no_new_foci_flag,

  -- Dominios clínicos diarios
  CASE WHEN d.Temp_median BETWEEN 36.5 AND 38.4 THEN 1 ELSE 0 END AS temp_in_range,
  CASE WHEN d.WBC_median BETWEEN 4 AND 11 THEN 1 ELSE 0 END AS wbc_normalizing,
  CASE WHEN d.MAP_median >= 65 THEN 1 ELSE 0 END AS hemo_stable,
  CASE
    WHEN d.Lactate_median IS NULL THEN NULL
    WHEN d.Lactate_median < 2 THEN 1 ELSE 0
  END AS lactate_normalizing,
  CASE
    WHEN d.spo2fio2_ratio IS NULL THEN NULL
    WHEN d.spo2fio2_ratio >= 240 THEN 1 ELSE 0
  END AS resp_improving

FROM daily d
LEFT JOIN new_foci nf
  ON d.stay_id = nf.stay_id
 AND d.day_idx = nf.day_idx
ORDER BY d.stay_id, d.day_idx;
