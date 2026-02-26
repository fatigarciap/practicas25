-- ============================================
-- 🧩 BLOQUE 0: DEFINICIÓN DE LA COHORTE (TABLA)
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.bloque_0_cohorte` AS

WITH MicrobiologyEvents AS (
  SELECT
    icu.stay_id,
    icu.hadm_id,
    micro.microevent_id,
    micro.org_name,
    micro.ab_name,
    micro.interpretation,
    micro.charttime,
    micro.subject_id,
    micro.spec_type_desc,
    CASE
      WHEN LOWER(micro.ab_name) LIKE '%ceftriaxone%' THEN 'Cefalosporinas_3ra'
      WHEN LOWER(micro.ab_name) LIKE '%cefepime%' THEN 'Cefalosporinas_4ta'
      WHEN LOWER(micro.ab_name) LIKE '%ceftazidime%' THEN 'Cefalosporinas_3ra'
      WHEN LOWER(micro.ab_name) LIKE '%meropenem%' THEN 'Carbapenems'
      WHEN LOWER(micro.ab_name) LIKE '%imipenem%' THEN 'Carbapenems'
      WHEN LOWER(micro.ab_name) LIKE '%trimeth%prim%sulfa%' THEN 'Sulfonamidas'
      WHEN LOWER(micro.ab_name) LIKE '%linezolid%' THEN 'Oxazolidinonas'
      WHEN LOWER(micro.ab_name) LIKE '%daptomycin%' THEN 'Lipopéptidos'
      WHEN LOWER(micro.ab_name) LIKE '%oxacillin%' THEN 'Penicilinas_antiestaf'
      WHEN LOWER(micro.ab_name) LIKE '%vancomycin%' THEN 'Glicopéptidos'
      ELSE 'Otra_clase'
    END AS antibiotic_class
  FROM `physionet-data.mimiciv_3_1_icu.icustays` icu
  INNER JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
    ON icu.hadm_id = micro.hadm_id
   AND micro.charttime BETWEEN icu.intime AND icu.outtime
  WHERE
    micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND micro.interpretation IN ('R','S','I')
    AND LOWER(micro.org_name) IN (
      'escherichia coli', 'klebsiella pneumoniae', 'klebsiella aerogenes',
      'enterobacter cloacae', 'enterobacter aerogenes', 'pseudomonas aeruginosa',
      'acinetobacter baumannii', 'stenotrophomonas maltophilia',
      'enterococcus faecium', 'staphylococcus aureus'
    )
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab', 'fluid,other', 'foreign body', 'foot culture',
      'fluid received in blood culture bottles', 'ear',
      'fluid wound', 'dialysis fluid', 'skin scrapings',
      'foreign body - sonication culture', 'eye'
    )
),

-- ✅ DEFINICIÓN CORRECTA DE MONOMICROBIANA
SingleOrganismEvents AS (
  SELECT
    m.stay_id,
    m.hadm_id,
    m.charttime,
    m.spec_type_desc,
    m.org_name,
    m.antibiotic_class,
    m.interpretation
  FROM MicrobiologyEvents m
  JOIN (
    SELECT
      stay_id,
      charttime,
      spec_type_desc,
      COUNT(DISTINCT org_name) AS n_orgs
    FROM MicrobiologyEvents
    GROUP BY
      stay_id,
      charttime,
      spec_type_desc
  ) c
    ON m.stay_id = c.stay_id
   AND m.charttime = c.charttime
   AND m.spec_type_desc = c.spec_type_desc
  WHERE c.n_orgs = 1
),

ComorbidityExclusions AS (
  SELECT DISTINCT hadm_id
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE icd_code LIKE 'N18%'
     OR icd_code LIKE 'E11%'
     OR icd_code LIKE 'I50%'
),

PriorTreatment AS (
  SELECT
    s.stay_id,
    s.hadm_id,
    s.charttime,
    s.spec_type_desc,
    s.org_name,
    s.antibiotic_class,
    s.interpretation,
    COUNT(p.starttime) > 0 AS has_prior_treatment
  FROM SingleOrganismEvents s
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON s.hadm_id = p.hadm_id
   AND p.starttime < s.charttime
   AND p.starttime >= DATETIME_SUB(s.charttime, INTERVAL 48 HOUR)
   AND LOWER(p.drug) IN (
      'ceftriaxone', 'cefepime', 'ceftazidime', 'meropenem', 'imipenem',
      'trimethoprim-sulfamethoxazole', 'linezolid', 'daptomycin',
      'oxacillin', 'vancomycin'
   )
  GROUP BY
    s.stay_id,
    s.hadm_id,
    s.charttime,
    s.spec_type_desc,
    s.org_name,
    s.antibiotic_class,
    s.interpretation
),

PostTreatment AS (
  SELECT
    p.stay_id,
    p.hadm_id,
    p.charttime,
    p.spec_type_desc,
    p.org_name,
    p.antibiotic_class,
    p.interpretation,
    p.has_prior_treatment,
    COUNT(p2.starttime) > 0 AS has_post_treatment
  FROM PriorTreatment p
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p2
    ON p.hadm_id = p2.hadm_id
   AND p2.starttime >= p.charttime
   AND p2.starttime <= DATETIME_ADD(p.charttime, INTERVAL 48 HOUR)
   AND LOWER(p2.drug) IN (
      'ceftriaxone', 'cefepime', 'ceftazidime', 'meropenem', 'imipenem',
      'trimethoprim-sulfamethoxazole', 'linezolid', 'daptomycin',
      'oxacillin', 'vancomycin'
   )
  GROUP BY
    p.stay_id,
    p.hadm_id,
    p.charttime,
    p.spec_type_desc,
    p.org_name,
    p.antibiotic_class,
    p.interpretation,
    p.has_prior_treatment
),

FilteredCohort AS (
  SELECT *
  FROM PostTreatment
  WHERE has_prior_treatment = FALSE
    AND has_post_treatment = TRUE
    AND hadm_id NOT IN (SELECT hadm_id FROM ComorbidityExclusions)
)

SELECT *
FROM FilteredCohort;
