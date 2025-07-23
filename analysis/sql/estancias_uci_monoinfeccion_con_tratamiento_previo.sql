-- estancias_uci_monoinfeccion_con_tratamiento_previo.sql

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
    END AS antibiotic_class,
    ROW_NUMBER() OVER (
      PARTITION BY icu.stay_id, micro.org_name
      ORDER BY
        CASE
          WHEN micro.interpretation = 'R' THEN 1
          WHEN micro.interpretation = 'S' THEN 2
          WHEN micro.interpretation = 'I' THEN 3
        END ASC,
        micro.charttime ASC
    ) AS RowNum
  FROM
    `physionet-data.mimiciv_3_1_icu.icustays` icu
  INNER JOIN
    `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
  ON
    icu.hadm_id = micro.hadm_id
  WHERE
    micro.interpretation IN ('R', 'S', 'I')
    AND micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND (
      LOWER(micro.org_name) LIKE '%escherichia coli%'
      OR LOWER(micro.org_name) LIKE '%klebsiella pneumoniae%'
      OR LOWER(micro.org_name) LIKE '%klebsiella aerogenes%'
      OR LOWER(micro.org_name) LIKE '%enterobacter cloacae%'
      OR LOWER(micro.org_name) LIKE '%enterobacter aerogenes%'
      OR LOWER(micro.org_name) LIKE '%pseudomonas aeruginosa%'
      OR LOWER(micro.org_name) LIKE '%acinetobacter baumannii%'
      OR LOWER(micro.org_name) LIKE '%stenotrophomonas maltophilia%'
      OR LOWER(micro.org_name) LIKE '%enterococcus faecium%'
      OR LOWER(micro.org_name) LIKE '%staphylococcus aureus%'
    )
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab', 'fluid,other', 'foreign body', 'foot culture',
      'fluid received in blood culture bottles', 'ear',
      'fluid wound', 'dialysis fluid', 'skin scrapings',
      'foreign body - sonication culture', 'eye'
    )
),
SingleOrganismEvents AS (
  SELECT
    stay_id,
    hadm_id,
    charttime,
    org_name,
    antibiotic_class,
    interpretation
  FROM
    MicrobiologyEvents
  WHERE
    RowNum = 1
  GROUP BY
    stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  HAVING
    COUNT(DISTINCT org_name) = 1
),
ComorbidityExclusions AS (
  SELECT
    hadm_id
  FROM
    `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE
    icd_code LIKE 'N18%' OR icd_code LIKE 'E11%' OR icd_code LIKE 'I50%'
),
PriorTreatment AS (
  SELECT
    s.stay_id,
    s.hadm_id,
    s.charttime,
    s.org_name,
    s.antibiotic_class,
    s.interpretation,
    CASE
      WHEN COUNT(p.starttime) > 0 THEN 1 ELSE 0
    END AS has_prior_treatment
  FROM
    SingleOrganismEvents s
  LEFT JOIN
    `physionet-data.mimiciv_3_1_hosp.prescriptions` p
  ON
    s.hadm_id = p.hadm_id
    AND p.starttime < s.charttime
    AND LOWER(p.drug) IN (
      'ceftriaxone', 'cefepime', 'ceftazidime', 'meropenem', 'imipenem',
      'trimethoprim-sulfamethoxazole', 'linezolid', 'daptomycin',
      'oxacillin', 'vancomycin'
    )
  GROUP BY
    s.stay_id, s.hadm_id, s.charttime, s.org_name, s.antibiotic_class, s.interpretation
),
FilteredStays AS (
  SELECT
    p.stay_id,
    p.hadm_id,
    p.charttime,
    p.org_name,
    p.antibiotic_class,
    p.interpretation,
    p.has_prior_treatment
  FROM
    PriorTreatment p
  LEFT JOIN
    ComorbidityExclusions c
  ON
    p.hadm_id = c.hadm_id
  WHERE
    c.hadm_id IS NULL
)
SELECT
  stay_id,
  hadm_id,
  charttime,
  org_name,
  antibiotic_class,
  interpretation,
  has_prior_treatment
FROM
  FilteredStays
LIMIT 10