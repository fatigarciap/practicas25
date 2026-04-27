CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` AS

WITH base AS (
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

    day_idx,
    window_start,
    window_end,

    n_abx_t0,
    spectrum_level_t0,
    has_broad_t0,
    has_gp_resistant_t0,
    has_gn_mdr_t0,

    COALESCE(t0_vancomycin,0) AS t0_vancomycin,
    COALESCE(t0_pip_tazo,0) AS t0_pip_tazo,
    COALESCE(t0_cefepime,0) AS t0_cefepime,
    COALESCE(t0_meropenem,0) AS t0_meropenem,
    COALESCE(t0_ceftriaxone,0) AS t0_ceftriaxone,
    COALESCE(t0_ciprofloxacin,0) AS t0_ciprofloxacin,
    COALESCE(t0_linezolid,0) AS t0_linezolid,
    COALESCE(t0_daptomycin,0) AS t0_daptomycin,
    COALESCE(t0_cefazolin,0) AS t0_cefazolin,
    COALESCE(t0_levofloxacin,0) AS t0_levofloxacin,
    COALESCE(t0_ampicillin,0) AS t0_ampicillin,
    COALESCE(t0_ampicillin_sulbactam,0) AS t0_ampicillin_sulbactam,

    COALESCE(t0_metronidazole,0) AS t0_metronidazole,
    COALESCE(t0_tobramycin,0) AS t0_tobramycin,
    COALESCE(t0_gentamicin,0) AS t0_gentamicin,
    COALESCE(t0_aztreonam,0) AS t0_aztreonam,
    COALESCE(t0_clindamycin,0) AS t0_clindamycin,
    COALESCE(t0_any_aminoglycoside,0) AS t0_any_aminoglycoside

FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
),

features AS (
SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,

    HR_median,
    MAP_median,
    SysBP_median,
    DiasBP_median,
    Temp_median,
    RR_median,
    SpO2_median,
    FiO2_median,
    WBC_median,
    Lactate_median,
    Creatinine_median,
    Bilirubin_median,
    Platelets_median,
    Hgb_median,
    spo2fio2_ratio

FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
),

domains AS (
SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,

    no_new_foci_flag,
    radiology_stable_flag,

    temp_in_range,
    wbc_normalizing,
    hemo_stable,
    lactate_normalizing,
    resp_improving

FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci`
),

flags AS (
SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,

    n_domains_ok,
    improved_today,
    sustained_improvement

FROM `strange-math-456415-c3.mimic_analysis.improvement_flags`
)

SELECT

b.subject_id,
b.hadm_id,
b.stay_id,
b.microevent_id,

b.t0,
b.index_charttime,
b.episode_anchor_source,

b.icu_intime,
b.icu_outtime,
b.deathtime,
b.followup_end,

b.specimen_type,
b.organism_name,
b.is_monomicrobial_event,

b.day_idx,
b.window_start,
b.window_end,

b.n_abx_t0,
b.spectrum_level_t0,
b.has_broad_t0,
b.has_gp_resistant_t0,
b.has_gn_mdr_t0,

b.t0_vancomycin,
b.t0_pip_tazo,
b.t0_cefepime,
b.t0_meropenem,
b.t0_ceftriaxone,
b.t0_ciprofloxacin,
b.t0_linezolid,
b.t0_daptomycin,
b.t0_cefazolin,
b.t0_levofloxacin,
b.t0_ampicillin,
b.t0_ampicillin_sulbactam,

b.t0_metronidazole,
b.t0_tobramycin,
b.t0_gentamicin,
b.t0_aztreonam,
b.t0_clindamycin,
b.t0_any_aminoglycoside,

f.HR_median,
f.MAP_median,
f.SysBP_median,
f.DiasBP_median,
f.Temp_median,
f.RR_median,
f.SpO2_median,
f.FiO2_median,
f.WBC_median,
f.Lactate_median,
f.Creatinine_median,
f.Bilirubin_median,
f.Platelets_median,
f.Hgb_median,
f.spo2fio2_ratio,

d.no_new_foci_flag,
d.radiology_stable_flag,
d.temp_in_range,
d.wbc_normalizing,
d.hemo_stable,
d.lactate_normalizing,
d.resp_improving,

fl.n_domains_ok,
fl.improved_today,
fl.sustained_improvement

FROM base b

LEFT JOIN features f
USING (subject_id, hadm_id, stay_id, day_idx)

LEFT JOIN domains d
USING (subject_id, hadm_id, stay_id, day_idx)

LEFT JOIN flags fl
USING (subject_id, hadm_id, stay_id, day_idx)

ORDER BY stay_id, day_idx;
