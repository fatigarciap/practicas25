-- estancias_uci_monoinfeccion.sql

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
),
SingleOrganismEvents AS (
  SELECT
    stay_id
  FROM
    MicrobiologyEvents
  WHERE
    RowNum = 1
  GROUP BY
    stay_id
  HAVING
    COUNT(DISTINCT org_name) = 1  -- Excluye infecciones polimicrobianas
)
SELECT
  COUNT(DISTINCT stay_id) AS total_estancias_uci_cultivos_monoinfeccion
FROM
  SingleOrganismEvents
