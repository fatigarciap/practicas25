-- 🎯 Objetivo:
-- Contar estancias UCI con cultivos monomicrobianos, sin comorbilidades graves,
-- clasificadas por si recibieron tratamiento antibiótico previo.

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
        CASE micro.interpretation
          WHEN 'R' THEN 1
          WHEN 'S' THEN 2
          WHEN 'I' THEN 3
        END,
        micro.charttime
    ) AS row_num
  FROM
    `physionet-data.mimiciv_3_1_icu.icustays` AS icu
  INNER JOIN
    `physionet-data.mimiciv_3_1_hosp.microbiologyevents` AS micro
    ON icu.hadm_id = micro.hadm_id
  WHERE
    micro.interpretation IN ('R', 'S', 'I')
    AND micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab', 'fluid,other', 'foreign body', 'foot culture',
      'fluid received in blood culture bottles', 'ear', 'fluid wound',
      'dialysis fluid', 'skin scrapings', 'foreign body - sonication culture', 'eye'
    )
    AND LOWER(micro.org_name) IN (
      'escherichia coli', 'klebsiella pneumoniae', 'klebsiella aerogenes',
      'enterobacter cloacae', 'enterobacter aerogenes', 'pseudomonas aeruginosa',
      'acinetobacter baumannii', 'stenotrophomonas maltophilia',
      'enterococcus faecium', 'staphylococcus aureus'
    )
),

SingleMicrobeEvents AS (
  SELECT
    stay_id,
    hadm_id,
    charttime,
    org_name,
    antibiotic_class,
    interpretation
  FROM
    MicrobiologyEvents
  WHERE row_num = 1
  GROUP BY
    stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  HAVING COUNT(DISTINCT org_name) = 1  -- Excluir infecciones polimicrobianas
),

ExclusionComorbidities AS (
  SELECT DISTINCT hadm_id
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE
    icd_code LIKE 'N18%'  -- Enfermedad renal crónica
    OR icd_code LIKE 'E11%' -- Diabetes tipo 2
    OR icd_code LIKE 'I50%' -- Insuficiencia cardíaca
),

PriorAntibioticTreatment AS (
  SELECT
    e.stay_id,
    e.hadm_id,
    e.charttime,
    e.org_name,
    e.antibiotic_class,
    e.interpretation,
    CASE
      WHEN COUNT(p.starttime) > 0 THEN 1 ELSE 0
    END AS has_prior_treatment
  FROM
    SingleMicrobeEvents e
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON e.hadm_id = p.hadm_id
    AND p.starttime < e.charttime
    AND LOWER(p.drug) IN (
      'ceftriaxone', 'cefepime', 'ceftazidime', 'meropenem', 'imipenem',
      'trimethoprim-sulfamethoxazole', 'linezolid', 'daptomycin',
      'oxacillin', 'vancomycin'
    )
  GROUP BY
    e.stay_id, e.hadm_id, e.charttime, e.org_name, e.antibiotic_class, e.interpretation
),

FilteredCases AS (
  SELECT
    t.*
  FROM PriorAntibioticTreatment t
  LEFT JOIN ExclusionComorbidities c
    ON t.hadm_id = c.hadm_id
  WHERE c.hadm_id IS NULL
)

-- Resultado final: conteo de estancias y tratamientos
SELECT
  COUNT(DISTINCT stay_id) AS total_estancias_uci_monoinfeccion,
  SUM(has_prior_treatment) AS total_con_tratamiento_previo,
  COUNT(DISTINCT CASE WHEN has_prior_treatment = 1 THEN stay_id END) AS estancias_distintas_con_tratamiento
FROM FilteredCases;
