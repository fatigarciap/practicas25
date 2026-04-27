CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_0_antibiogram_detail_clean` AS

SELECT DISTINCT
  b.stay_id,
  b.hadm_id,
  b.microevent_id,
  CAST(m.charttime AS TIMESTAMP) AS index_charttime,
  LOWER(m.spec_type_desc) AS specimen_type,
  LOWER(m.org_name) AS organism_name,
  LOWER(m.ab_name) AS susceptibility_ab_name,
  m.interpretation AS susceptibility_interpretation
FROM `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean` b
JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` m
  ON b.hadm_id = m.hadm_id
 AND b.microevent_id = m.microevent_id
WHERE m.ab_name IS NOT NULL
  AND m.interpretation IN ('R','S','I');