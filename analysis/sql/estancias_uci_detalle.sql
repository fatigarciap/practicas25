SELECT
  icu.stay_id,
  icu.hadm_id,
  micro.org_name,
  micro.charttime,
  micro.ab_name,
  micro.interpretation
FROM
  `physionet-data.mimiciv_3_1_icu.icustays` icu
INNER JOIN
  `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
ON
  icu.hadm_id = micro.hadm_id
WHERE
  micro.hadm_id IS NOT NULL
LIMIT 10

