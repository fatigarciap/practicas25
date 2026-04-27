CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean` AS

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY stay_id
      ORDER BY index_charttime ASC, microevent_id ASC
    ) AS rn
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean`
)

SELECT
  subject_id,
  hadm_id,
  stay_id,
  microevent_id,
  index_charttime,
  episode_anchor_source,
  icu_intime,
  icu_outtime,
  index_within_icu,
  hours_from_icu_intime_to_index,
  hours_from_index_to_icu_outtime,
  specimen_type,
  organism_name,
  is_monomicrobial_event
FROM ranked
WHERE rn = 1;