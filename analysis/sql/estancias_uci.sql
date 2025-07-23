SELECT
  COUNT(DISTINCT icu.stay_id) AS total_estancias_uci
FROM
  `physionet-data.mimiciv_3_1_icu.icustays` icu
INNER JOIN
  `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
ON
  icu.hadm_id = micro.hadm_id
WHERE
  micro.hadm_id IS NOT NULL
