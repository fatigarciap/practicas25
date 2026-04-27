CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.new_foci_flag_clean` AS

WITH first_new_foci_day AS (

SELECT
w.stay_id,
MIN(w.day_idx) first_new_foci_day

FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` w

JOIN `strange-math-456415-c3.mimic_analysis.new_foci_events_clean` n
ON w.stay_id = n.stay_id
AND n.charttime >= w.window_start
AND n.charttime < w.window_end

GROUP BY w.stay_id
)

SELECT
w.subject_id,
w.hadm_id,
w.stay_id,
w.day_idx,

CASE
WHEN f.first_new_foci_day IS NULL THEN 1
WHEN w.day_idx < f.first_new_foci_day THEN 1
ELSE 0
END no_new_foci_flag

FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` w

LEFT JOIN first_new_foci_day f
ON w.stay_id = f.stay_id;