CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` AS

WITH cohort AS (
  SELECT
    t.subject_id,
    t.hadm_id,
    t.stay_id,
    t.microevent_id,
    t.true_t0 AS t0,
    t.index_charttime,
    t.episode_anchor_source,
    t.icu_intime,
    t.icu_outtime,
    t.specimen_type,
    t.organism_name,
    t.is_monomicrobial_event
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true` t
),

regimen_summary AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
),

regimen_multihot AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
),

cohort_enriched AS (
  SELECT
    c.*,

    r.n_abx_t0,
    r.spectrum_level_t0,
    r.has_broad_t0,
    r.has_gp_resistant_t0,
    r.has_gn_mdr_t0,

    m.t0_vancomycin,
    m.t0_pip_tazo,
    m.t0_cefepime,
    m.t0_meropenem,
    m.t0_ceftriaxone,
    m.t0_ciprofloxacin,
    m.t0_linezolid,
    m.t0_daptomycin,
    m.t0_cefazolin,
    m.t0_levofloxacin,
    m.t0_ampicillin,
    m.t0_ampicillin_sulbactam,
    m.t0_metronidazole,
    m.t0_tobramycin,
    m.t0_gentamicin,
    m.t0_aztreonam,
    m.t0_clindamycin,
    m.t0_any_aminoglycoside

  FROM cohort c

  LEFT JOIN regimen_summary r
    USING (stay_id)

  LEFT JOIN regimen_multihot m
    USING (stay_id)
),

icu_context AS (
  SELECT
    c.*,
    CAST(a.deathtime AS TIMESTAMP) AS deathtime
  FROM cohort_enriched c
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.admissions` a
    ON c.hadm_id = a.hadm_id
),

followup AS (
  SELECT
    *,
    LEAST(
      icu_outtime,
      IFNULL(deathtime, icu_outtime),
      TIMESTAMP_ADD(t0, INTERVAL 30 DAY)
    ) AS followup_end
  FROM icu_context
  WHERE t0 < LEAST(
    icu_outtime,
    IFNULL(deathtime, icu_outtime),
    TIMESTAMP_ADD(t0, INTERVAL 30 DAY)
  )
),

expanded_days AS (
  SELECT
    *,
    GENERATE_ARRAY(
      0,
      CAST(FLOOR(TIMESTAMP_DIFF(followup_end, t0, HOUR) / 24) AS INT64)
    ) AS day_indices
  FROM followup
),

windows AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    microevent_id,

    t0,
    index_charttime,
    episode_anchor_source,

    icu_intime,
    icu_outtime,
    deathtime,
    followup_end,

    specimen_type,
    organism_name,
    is_monomicrobial_event,

    n_abx_t0,
    spectrum_level_t0,
    has_broad_t0,
    has_gp_resistant_t0,
    has_gn_mdr_t0,

    t0_vancomycin,
    t0_pip_tazo,
    t0_cefepime,
    t0_meropenem,
    t0_ceftriaxone,
    t0_ciprofloxacin,
    t0_linezolid,
    t0_daptomycin,
    t0_cefazolin,
    t0_levofloxacin,
    t0_ampicillin,
    t0_ampicillin_sulbactam,
    t0_metronidazole,
    t0_tobramycin,
    t0_gentamicin,
    t0_aztreonam,
    t0_clindamycin,
    t0_any_aminoglycoside,

    day_idx,

    TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) AS window_start,

    LEAST(
      TIMESTAMP_ADD(t0, INTERVAL (day_idx + 1) * 24 HOUR),
      followup_end
    ) AS window_end

  FROM expanded_days,
  UNNEST(day_indices) AS day_idx

  WHERE TIMESTAMP_ADD(t0, INTERVAL day_idx * 24 HOUR) < followup_end
)

SELECT *
FROM windows
ORDER BY stay_id, day_idx;