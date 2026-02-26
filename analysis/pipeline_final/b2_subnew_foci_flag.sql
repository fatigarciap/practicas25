CREATE OR REPLACE TABLE
  `strange-math-456415-c3.mimic_analysis.new_foci_flag` AS

WITH
-- 1) Baseline TIME = t0 (inicio antibiótico) del día 0
baseline_time AS (
  SELECT
    stay_id,
    ANY_VALUE(t0) AS baseline_time
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
  WHERE day_idx = 0
  GROUP BY stay_id
),

-- 2) Baseline ORG = germen índice (del bloque 0)
baseline_org AS (
  SELECT
    stay_id,
    ANY_VALUE(org_name) AS baseline_org
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0_cohorte`
  GROUP BY stay_id
),

-- 3) Cultivos durante el ingreso (microbiologyevents → icustays por hadm_id)
followup_cultures AS (
  SELECT
    icu.stay_id,
    m.charttime,
    m.org_name
  FROM `physionet-data.mimiciv_3_1_hosp.microbiologyevents` m
  JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu
    ON m.hadm_id = icu.hadm_id
  WHERE m.interpretation IN ('R','S','I')
    AND m.org_name IS NOT NULL
    AND m.charttime IS NOT NULL
),

-- 4) Eventos de nuevo foco: después de baseline_time y germen distinto al índice
new_foci_events AS (
  SELECT DISTINCT
    f.stay_id,
    f.charttime
  FROM followup_cultures f
  JOIN baseline_time bt
    ON f.stay_id = bt.stay_id
  JOIN baseline_org bo
    ON f.stay_id = bo.stay_id
  WHERE CAST(f.charttime AS TIMESTAMP) > bt.baseline_time
    AND f.org_name != bo.baseline_org
),

-- 5) Primer día (day_idx) donde aparece el nuevo foco
first_new_foci_day AS (
  SELECT
    w.stay_id,
    MIN(w.day_idx) AS first_new_foci_day
  FROM `strange-math-456415-c3.mimic_analysis.base_windows` w
  JOIN new_foci_events n
    ON w.stay_id = n.stay_id
   AND CAST(n.charttime AS TIMESTAMP) >= w.window_start
   AND CAST(n.charttime AS TIMESTAMP) <  w.window_end
  GROUP BY w.stay_id
)

-- 6) Flag diario final
SELECT
  w.stay_id,
  w.day_idx,
  CASE
    WHEN w.day_idx = 0 THEN 1
    WHEN f.first_new_foci_day IS NULL THEN 1
    WHEN w.day_idx < f.first_new_foci_day THEN 1
    ELSE 0
  END AS no_new_foci_flag
FROM `strange-math-456415-c3.mimic_analysis.base_windows` w
LEFT JOIN first_new_foci_day f
  ON w.stay_id = f.stay_id
ORDER BY w.stay_id, w.day_idx;