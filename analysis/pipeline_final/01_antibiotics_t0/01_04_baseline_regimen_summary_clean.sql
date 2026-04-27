CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean` AS

WITH regimen AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
),

summary AS (
  SELECT
    stay_id,
    hadm_id,
    subject_id,
    microevent_id,
    true_t0,

    COUNT(*) AS n_abx_t0,

    MAX(spectrum_level) AS spectrum_level_t0,

    MAX(CASE WHEN spectrum_level >= 3 THEN 1 ELSE 0 END) AS has_broad_t0,

    MAX(CASE WHEN coverage_domain = 'gp_resistente' THEN 1 ELSE 0 END) AS has_gp_resistant_t0,

    MAX(CASE WHEN coverage_domain = 'gn_multirresistente' THEN 1 ELSE 0 END) AS has_gn_mdr_t0

  FROM regimen
  GROUP BY
    stay_id,
    hadm_id,
    subject_id,
    microevent_id,
    true_t0
)

SELECT *
FROM summary;