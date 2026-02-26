CREATE OR REPLACE TABLE
  `strange-math-456415-c3.mimic_analysis.improvement_flags` AS

WITH base AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    no_new_foci_flag,
    temp_in_range,
    wbc_normalizing,
    hemo_stable,
    lactate_normalizing,
    resp_improving
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci`
),

scored AS (
  SELECT
    *,
    (temp_in_range
     + wbc_normalizing
     + hemo_stable
     + COALESCE(lactate_normalizing,0)
     + COALESCE(resp_improving,0)
    ) AS n_domains_ok
  FROM base
),

daily_flag AS (
  SELECT
    *,
    CASE
      WHEN no_new_foci_flag = 1
       AND n_domains_ok >= 3
      THEN 1 ELSE 0
    END AS improved_today
  FROM scored
),

sustained AS (
  SELECT
    *,
    CASE
      WHEN improved_today = 1
       AND LAG(improved_today) OVER (
         PARTITION BY stay_id ORDER BY day_idx
       ) = 1
      THEN 1 ELSE 0
    END AS sustained_improvement
  FROM daily_flag
)

SELECT *
FROM sustained
ORDER BY stay_id, day_idx;
