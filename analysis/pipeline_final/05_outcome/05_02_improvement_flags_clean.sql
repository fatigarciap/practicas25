CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.improvement_flags_clean` AS

WITH base AS (

SELECT
subject_id,
hadm_id,
stay_id,
day_idx,

no_new_foci_flag,
radiology_stable_flag,
temp_in_range,
wbc_normalizing,
hemo_stable,
lactate_normalizing,
resp_improving

FROM mimic_analysis.clinical_domains_sci_clean
),

scored AS (

SELECT
*,

(
COALESCE(temp_in_range,0)
+ COALESCE(wbc_normalizing,0)
+ COALESCE(hemo_stable,0)
+ COALESCE(lactate_normalizing,0)
+ COALESCE(resp_improving,0)
) n_domains_ok

FROM base
),

daily_flag AS (

SELECT
*,

CASE
WHEN COALESCE(no_new_foci_flag,1)=1
AND COALESCE(radiology_stable_flag,1)=1
AND n_domains_ok >= 3
THEN 1
ELSE 0
END improved_today

FROM scored
),

sustained AS (

SELECT
*,

CASE
WHEN improved_today = 1
AND LAG(improved_today) OVER(
PARTITION BY stay_id
ORDER BY day_idx
)=1
THEN 1
ELSE 0
END sustained_improvement

FROM daily_flag
)

SELECT *
FROM sustained;
