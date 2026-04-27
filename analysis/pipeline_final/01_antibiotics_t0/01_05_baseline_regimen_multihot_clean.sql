CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean` AS

WITH regimen AS (
  SELECT
    stay_id,
    abx_name_std
  FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
),

pivoted AS (
  SELECT
    stay_id,

    MAX(IF(abx_name_std = 'vancomycin_iv',1,0)) AS t0_vancomycin,
    MAX(IF(abx_name_std = 'pip_tazo',1,0)) AS t0_pip_tazo,
    MAX(IF(abx_name_std = 'cefepime',1,0)) AS t0_cefepime,
    MAX(IF(abx_name_std = 'meropenem',1,0)) AS t0_meropenem,
    MAX(IF(abx_name_std = 'ceftriaxone',1,0)) AS t0_ceftriaxone,
    MAX(IF(abx_name_std = 'ciprofloxacin',1,0)) AS t0_ciprofloxacin,
    MAX(IF(abx_name_std = 'linezolid',1,0)) AS t0_linezolid,
    MAX(IF(abx_name_std = 'daptomycin',1,0)) AS t0_daptomycin,
    MAX(IF(abx_name_std = 'cefazolin',1,0)) AS t0_cefazolin,
    MAX(IF(abx_name_std = 'levofloxacin',1,0)) AS t0_levofloxacin,
    MAX(IF(abx_name_std = 'ampicillin',1,0)) AS t0_ampicillin,
    MAX(IF(abx_name_std = 'ampicillin_sulbactam',1,0)) AS t0_ampicillin_sulbactam,

    MAX(IF(abx_name_std = 'metronidazole',1,0)) AS t0_metronidazole,
    MAX(IF(abx_name_std = 'tobramycin',1,0)) AS t0_tobramycin,
    MAX(IF(abx_name_std = 'gentamicin',1,0)) AS t0_gentamicin,
    MAX(IF(abx_name_std = 'aztreonam',1,0)) AS t0_aztreonam,
    MAX(IF(abx_name_std = 'clindamycin',1,0)) AS t0_clindamycin,

    MAX(IF(abx_name_std IN ('gentamicin','tobramycin','amikacin'),1,0)) AS t0_any_aminoglycoside

  FROM regimen
  GROUP BY stay_id
)

SELECT *
FROM pivoted;