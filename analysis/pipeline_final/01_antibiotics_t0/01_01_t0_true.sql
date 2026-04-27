CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_t0_true` AS

WITH index_cohort AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    microevent_id,
    CAST(index_charttime AS TIMESTAMP) AS index_charttime,
    episode_anchor_source,
    CAST(icu_intime AS TIMESTAMP) AS icu_intime,
    CAST(icu_outtime AS TIMESTAMP) AS icu_outtime,
    specimen_type,
    organism_name,
    is_monomicrobial_event
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
),

rx_candidates AS (
  SELECT
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    c.microevent_id,
    c.index_charttime,
    c.episode_anchor_source,
    c.icu_intime,
    c.icu_outtime,
    c.specimen_type,
    c.organism_name,
    c.is_monomicrobial_event,
    LOWER(p.drug) AS drug_raw,
    CAST(p.starttime AS TIMESTAMP) AS start_ts,
    CAST(p.stoptime AS TIMESTAMP) AS stop_ts
  FROM index_cohort c
  JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON c.hadm_id = p.hadm_id
  WHERE p.drug IS NOT NULL
    AND p.starttime IS NOT NULL
    AND CAST(p.starttime AS TIMESTAMP) BETWEEN TIMESTAMP_SUB(c.index_charttime, INTERVAL 48 HOUR)
                                          AND TIMESTAMP_ADD(c.index_charttime, INTERVAL 48 HOUR)
    AND NOT REGEXP_CONTAINS(LOWER(p.drug), r'oral')
    AND NOT REGEXP_CONTAINS(LOWER(p.drug), r'enema')
    AND NOT REGEXP_CONTAINS(LOWER(p.drug), r'flush')
    AND NOT REGEXP_CONTAINS(LOWER(p.drug), r'dwell')
),

rx_mapped_candidates AS (
  SELECT
    r.*,
    m.match_priority,
    m.abx_name_std,
    m.spectrum_level,
    m.spectrum_label,
    m.coverage_domain
  FROM rx_candidates r
  JOIN `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean` m
    ON REGEXP_CONTAINS(r.drug_raw, m.pattern)
),

rx_mapped_dedup AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY stay_id, hadm_id, start_ts, IFNULL(stop_ts, TIMESTAMP '9999-12-31 00:00:00 UTC'), drug_raw
        ORDER BY match_priority ASC, abx_name_std ASC
      ) AS rn
    FROM rx_mapped_candidates
  )
  WHERE rn = 1
),

t0_per_stay AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    microevent_id,
    index_charttime,
    episode_anchor_source,
    icu_intime,
    icu_outtime,
    specimen_type,
    organism_name,
    is_monomicrobial_event,
    MIN(start_ts) AS true_t0
  FROM rx_mapped_dedup
  GROUP BY
    subject_id,
    hadm_id,
    stay_id,
    microevent_id,
    index_charttime,
    episode_anchor_source,
    icu_intime,
    icu_outtime,
    specimen_type,
    organism_name,
    is_monomicrobial_event
)

SELECT *
FROM t0_per_stay;