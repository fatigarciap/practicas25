CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean` AS

WITH microbiology_icu AS (
  SELECT
    icu.subject_id,
    icu.hadm_id,
    icu.stay_id,
    CAST(icu.intime AS TIMESTAMP) AS icu_intime,
    CAST(icu.outtime AS TIMESTAMP) AS icu_outtime,
    micro.microevent_id,
    CAST(micro.charttime AS TIMESTAMP) AS micro_charttime,
    LOWER(micro.spec_type_desc) AS specimen_type,
    LOWER(micro.org_name) AS organism_name
  FROM `physionet-data.mimiciv_3_1_icu.icustays` icu
  INNER JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
    ON icu.hadm_id = micro.hadm_id
   AND CAST(micro.charttime AS TIMESTAMP) BETWEEN CAST(icu.intime AS TIMESTAMP) AND CAST(icu.outtime AS TIMESTAMP)
  WHERE micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND LOWER(micro.org_name) IN (
      'escherichia coli',
      'klebsiella pneumoniae',
      'klebsiella aerogenes',
      'enterobacter cloacae',
      'enterobacter aerogenes',
      'pseudomonas aeruginosa',
      'acinetobacter baumannii',
      'stenotrophomonas maltophilia',
      'enterococcus faecium',
      'staphylococcus aureus'
    )
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab',
      'fluid,other',
      'foreign body',
      'foot culture',
      'fluid received in blood culture bottles',
      'ear',
      'fluid wound',
      'dialysis fluid',
      'skin scrapings',
      'foreign body - sonication culture',
      'eye'
    )
),

event_level AS (
  SELECT DISTINCT
    subject_id,
    hadm_id,
    stay_id,
    icu_intime,
    icu_outtime,
    microevent_id,
    micro_charttime,
    specimen_type,
    organism_name
  FROM microbiology_icu
),

monomicrobial_events AS (
  SELECT
    stay_id,
    micro_charttime,
    specimen_type,
    COUNT(DISTINCT organism_name) AS n_orgs
  FROM event_level
  GROUP BY stay_id, micro_charttime, specimen_type
),

eligible_events AS (
  SELECT
    e.*,
    TRUE AS is_monomicrobial_event
  FROM event_level e
  INNER JOIN monomicrobial_events m
    ON e.stay_id = m.stay_id
   AND e.micro_charttime = m.micro_charttime
   AND e.specimen_type = m.specimen_type
  WHERE m.n_orgs = 1
)

SELECT
  subject_id,
  hadm_id,
  stay_id,
  microevent_id,
  micro_charttime AS index_charttime,
  'microbiology_charttime' AS episode_anchor_source,
  icu_intime,
  icu_outtime,
  TRUE AS index_within_icu,
  TIMESTAMP_DIFF(micro_charttime, icu_intime, HOUR) AS hours_from_icu_intime_to_index,
  TIMESTAMP_DIFF(icu_outtime, micro_charttime, HOUR) AS hours_from_index_to_icu_outtime,
  specimen_type,
  organism_name,
  is_monomicrobial_event
FROM eligible_events;