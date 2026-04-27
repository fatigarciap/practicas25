CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean` AS

WITH daily AS (
SELECT * FROM mimic_analysis.daily_features_clean
),

new_foci AS (
SELECT * FROM mimic_analysis.new_foci_flag_clean
),

radiology AS (
SELECT * FROM mimic_analysis.radiology_flag_clean
)

SELECT

d.subject_id,
d.hadm_id,
d.stay_id,
d.day_idx,

nf.no_new_foci_flag,
rf.radiology_stable_flag,

CASE
WHEN d.Temp_median BETWEEN 36 AND 38 THEN 1
WHEN d.Temp_median IS NULL THEN NULL
ELSE 0
END temp_in_range,

CASE
WHEN d.WBC_median BETWEEN 4 AND 12 THEN 1
WHEN d.WBC_median IS NULL THEN NULL
ELSE 0
END wbc_normalizing,

CASE
WHEN d.MAP_median >= 65 THEN 1
WHEN d.MAP_median IS NULL THEN NULL
ELSE 0
END hemo_stable,

CASE
WHEN d.Lactate_median < 2 THEN 1
WHEN d.Lactate_median IS NULL THEN NULL
ELSE 0
END lactate_normalizing,

CASE
WHEN d.spo2fio2_ratio >= 240 THEN 1
WHEN d.spo2fio2_ratio IS NULL THEN NULL
ELSE 0
END resp_improving

FROM daily d

LEFT JOIN new_foci nf
USING(subject_id,hadm_id,stay_id,day_idx)

LEFT JOIN radiology rf
USING(subject_id,hadm_id,stay_id,day_idx);