CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.radiology_flag_clean` AS

WITH first_radiology_worsening_day AS (
SELECT
    w.stay_id,
    MIN(w.day_idx) AS first_radiology_worsening_day
FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` w
JOIN `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean` r
ON w.stay_id = r.stay_id
AND r.charttime >= w.window_start
AND r.charttime < w.window_end
GROUP BY w.stay_id
)

SELECT
w.subject_id,
w.hadm_id,
w.stay_id,
w.day_idx,

CASE
WHEN f.first_radiology_worsening_day IS NULL THEN 1
WHEN w.day_idx < f.first_radiology_worsening_day THEN 1
ELSE 0
END AS radiology_stable_flag

FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` w

LEFT JOIN first_radiology_worsening_day f
ON w.stay_id = f.stay_id

ORDER BY w.stay_id, w.day_idx;