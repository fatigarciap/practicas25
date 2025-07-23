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
        END,
        micro.charttime
    ) AS RowNum
  FROM
    `physionet-data.mimiciv_3_1_icu.icustays` icu
  INNER JOIN
    `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
    ON icu.hadm_id = micro.hadm_id
  WHERE
    micro.interpretation IN ('R', 'S', 'I')
    AND micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
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
SingleOrganismEvents AS (
  SELECT
    stay_id, hadm_id, subject_id, charttime, org_name,
    antibiotic_class, interpretation
  FROM
    MicrobiologyEvents
  WHERE
    RowNum = 1
  GROUP BY
    stay_id, hadm_id, subject_id, charttime, org_name,
    antibiotic_class, interpretation
  HAVING
    COUNT(DISTINCT org_name) = 1
),
ComorbidityExclusions AS (
  SELECT DISTINCT hadm_id
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE icd_code LIKE 'N18%' OR icd_code LIKE 'E11%' OR icd_code LIKE 'I50%'
),
PriorTreatment AS (
  SELECT
    s.*,
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
  GROUP BY s.stay_id, s.hadm_id, s.subject_id, s.charttime, s.org_name,
           s.antibiotic_class, s.interpretation
),
PostTreatment AS (
  SELECT
    p.*,
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
  GROUP BY p.stay_id, p.hadm_id, p.subject_id, p.charttime, p.org_name,
           p.antibiotic_class, p.interpretation, p.has_prior_treatment
),
Filtered AS (
  SELECT *
  FROM PostTreatment
  WHERE has_prior_treatment = FALSE AND has_post_treatment = TRUE
),
BloquesClasificados AS (
  SELECT *,
    CASE
      WHEN LOWER(org_name) IN ('escherichia coli', 'klebsiella pneumoniae', 'klebsiella aerogenes',
                               'enterobacter cloacae', 'enterobacter aerogenes')
           AND LOWER(antibiotic_class) IN ('cefalosporinas_3ra', 'cefalosporinas_4ta') THEN 'Bloque 1'
      WHEN LOWER(org_name) IN ('escherichia coli', 'klebsiella pneumoniae', 'klebsiella aerogenes',
                               'enterobacter cloacae', 'enterobacter aerogenes')
           AND LOWER(antibiotic_class) = 'carbapenems' THEN 'Bloque 2'
      WHEN LOWER(org_name) IN ('pseudomonas aeruginosa', 'acinetobacter baumannii')
           AND LOWER(antibiotic_class) = 'carbapenems' THEN 'Bloque 3'
      WHEN LOWER(org_name) = 'stenotrophomonas maltophilia'
           AND LOWER(antibiotic_class) = 'sulfonamidas' THEN 'Bloque 4'
      WHEN LOWER(org_name) = 'enterococcus faecium'
           AND LOWER(antibiotic_class) IN ('oxazolidinonas', 'lipopéptidos') THEN 'Bloque 5'
      WHEN LOWER(org_name) = 'staphylococcus aureus'
           AND LOWER(antibiotic_class) IN ('penicilinas_antiestaf', 'glicopéptidos') THEN 'Bloque 6'
      ELSE 'Otros'
    END AS bloque
  FROM Filtered
),
TreatmentInfo AS (
  SELECT
    s.bloque,
    s.stay_id,
    s.hadm_id,
    s.subject_id,
    p.gender,
    DATETIME_DIFF(i.outtime, i.intime, DAY) AS estancia_dias,

    ROUND(AVG(
      CASE
        WHEN r.starttime >= i.intime AND r.stoptime <= i.outtime THEN TIMESTAMP_DIFF(r.stoptime, r.starttime, HOUR)
        ELSE NULL
      END
    ), 1) AS duracion_tratamiento,

    MAX(
      CASE
        WHEN r.starttime >= DATETIME_SUB(i.intime, INTERVAL 48 HOUR)
             AND r.starttime < i.intime
             AND LOWER(r.drug) IN (
                'ceftriaxone', 'cefepime', 'ceftazidime', 'meropenem', 'imipenem',
                'trimethoprim-sulfamethoxazole', 'linezolid', 'daptomycin',
                'oxacillin', 'vancomycin'
             ) THEN 1
        ELSE 0
      END
    ) AS tratamiento_previo,

    CASE
      WHEN a.deathtime IS NOT NULL
           AND a.deathtime >= i.intime
           AND a.deathtime <= i.outtime THEN 1
      ELSE 0
    END AS fallecio_en_uci,

    CASE
      WHEN a.deathtime IS NULL AND DATETIME_DIFF(i.outtime, i.intime, DAY) <= 30 THEN 1
      ELSE 0
    END AS mejoria

  FROM BloquesClasificados s
  JOIN `physionet-data.mimiciv_3_1_hosp.patients` p ON s.subject_id = p.subject_id
  JOIN `physionet-data.mimiciv_3_1_hosp.admissions` a ON s.hadm_id = a.hadm_id
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` i ON s.hadm_id = i.hadm_id AND s.stay_id = i.stay_id
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` r
    ON s.hadm_id = r.hadm_id
    AND r.starttime IS NOT NULL AND r.stoptime IS NOT NULL

  GROUP BY s.bloque, s.stay_id, s.hadm_id, s.subject_id, p.gender, i.intime, i.outtime, a.deathtime
)

SELECT
  bloque,
  COUNT(DISTINCT stay_id) AS num_pacientes,
  ROUND(SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS porcentaje_hombres,
  ROUND(SUM(CASE WHEN estancia_dias IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS porcentaje_uci,
  ROUND(AVG(estancia_dias), 1) AS estancia_media,
  ROUND(STDDEV(estancia_dias), 1) AS desviacion_estancia,
  ROUND(AVG(duracion_tratamiento), 1) AS duracion_tratamiento_horas,
  ROUND(SUM(fallecio_en_uci) / COUNT(*) * 100, 1) AS mortalidad_pct,
  ROUND(SUM(mejoria) / COUNT(*) * 100, 1) AS proporcion_mejora_pct
FROM TreatmentInfo
WHERE estancia_dias IS NOT NULL AND bloque != 'Otros'
GROUP BY bloque
ORDER BY bloque;
