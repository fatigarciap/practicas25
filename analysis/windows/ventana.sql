-- PLANTILLA: Ventanas 24h y agregados por paciente y bloque
WITH

-- (1) Microbiología
MicrobiologyEvents AS (
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
        CASE WHEN micro.interpretation = 'R' THEN 1
             WHEN micro.interpretation = 'S' THEN 2
             WHEN micro.interpretation = 'I' THEN 3 END,
        micro.charttime
    ) AS RowNum
  FROM `physionet-data.mimiciv_3_1_icu.icustays` icu
  INNER JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
    ON icu.hadm_id = micro.hadm_id
  WHERE micro.interpretation IN ('R','S','I')
    AND micro.charttime IS NOT NULL
    AND micro.hadm_id IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND LOWER(micro.org_name) IN (
      'escherichia coli','klebsiella pneumoniae','klebsiella aerogenes',
      'enterobacter cloacae','enterobacter aerogenes','pseudomonas aeruginosa',
      'acinetobacter baumannii','stenotrophomonas maltophilia',
      'enterococcus faecium','staphylococcus aureus'
    )
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab','fluid,other','foreign body','foot culture',
      'fluid received in blood culture bottles','ear','fluid wound','dialysis fluid',
      'skin scrapings','foreign body - sonication culture','eye'
    )
),

-- (2) Eventos con un solo organismo
SingleOrganismEvents AS (
  SELECT stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  FROM MicrobiologyEvents
  WHERE RowNum = 1
  GROUP BY stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  HAVING COUNT(DISTINCT org_name) = 1
),

-- (3) Exclusión por comorbilidad
ComorbidityExclusions AS (
  SELECT DISTINCT hadm_id
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE icd_code LIKE 'N18%' OR icd_code LIKE 'E11%' OR icd_code LIKE 'I50%'
),

-- (4) Tratamiento previo
PriorTreatment AS (
  SELECT s.*,
    COUNT(p.starttime) > 0 AS has_prior_treatment
  FROM SingleOrganismEvents s
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON s.hadm_id = p.hadm_id
    AND p.starttime < s.charttime
    AND p.starttime >= TIMESTAMP_SUB(s.charttime, INTERVAL 48 HOUR)
    AND LOWER(p.drug) IN ('ceftriaxone','cefepime','ceftazidime','meropenem','imipenem','trimethoprim-sulfamethoxazole','linezolid','daptomycin','oxacillin','vancomycin')
  GROUP BY s.stay_id, s.hadm_id, s.charttime, s.org_name, s.antibiotic_class, s.interpretation
),

-- (5) Tratamiento posterior
PostTreatment AS (
  SELECT p.*,
    COUNT(p2.starttime) > 0 AS has_post_treatment
  FROM PriorTreatment p
  LEFT JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p2
    ON p.hadm_id = p2.hadm_id
    AND p2.starttime >= p.charttime
    AND p2.starttime <= TIMESTAMP_ADD(p.charttime, INTERVAL 48 HOUR)
    AND LOWER(p2.drug) IN ('ceftriaxone','cefepime','ceftazidime','meropenem','imipenem','trimethoprim-sulfamethoxazole','linezolid','daptomycin','oxacillin','vancomycin')
  GROUP BY p.stay_id, p.hadm_id, p.charttime, p.org_name, p.antibiotic_class, p.interpretation, p.has_prior_treatment
),

-- (6) Filtrado: solo pacientes con tratamiento post-cultivo y sin previo
Filtered AS (
  SELECT * FROM PostTreatment
  WHERE has_prior_treatment = FALSE AND has_post_treatment = TRUE
),

-- (7) Clasificación por bloque
BloquesClasificados AS (
  SELECT * ,
    CASE
      WHEN LOWER(org_name) IN ('escherichia coli','klebsiella pneumoniae','klebsiella aerogenes','enterobacter cloacae','enterobacter aerogenes')
           AND LOWER(antibiotic_class) IN ('cefalosporinas_3ra','cefalosporinas_4ta') THEN 'Bloque 1'
      WHEN LOWER(org_name) IN ('escherichia coli','klebsiella pneumoniae','klebsiella aerogenes','enterobacter cloacae','enterobacter aerogenes')
           AND LOWER(antibiotic_class) = 'carbapenems' THEN 'Bloque 2'
      WHEN LOWER(org_name) IN ('pseudomonas aeruginosa','acinetobacter baumannii')
           AND LOWER(antibiotic_class) = 'carbapenems' THEN 'Bloque 3'
      WHEN LOWER(org_name) = 'stenotrophomonas maltophilia'
           AND LOWER(antibiotic_class) = 'sulfonamidas' THEN 'Bloque 4'
      WHEN LOWER(org_name) = 'enterococcus faecium'
           AND LOWER(antibiotic_class) IN ('oxazolidinonas','lipopéptidos') THEN 'Bloque 5'
      WHEN LOWER(org_name) = 'staphylococcus aureus'
           AND LOWER(antibiotic_class) IN ('penicilinas_antiestaf','glicopéptidos') THEN 'Bloque 6'
      ELSE 'Otros'
    END AS bloque
  FROM Filtered
),

-- (8) Primer inicio de antibiótico
FirstAbx AS (
  SELECT
    b.*,
    (SELECT MIN(p.starttime)
     FROM `physionet-data.mimiciv_3_1_hosp.prescriptions` p
     WHERE p.hadm_id = b.hadm_id
       AND p.starttime >= b.charttime
       AND p.starttime <= TIMESTAMP_ADD(b.charttime, INTERVAL 48 HOUR)
       AND LOWER(p.drug) IN ('ceftriaxone','cefepime','ceftazidime','meropenem','imipenem','trimethoprim-sulfamethoxazole','linezolid','daptomycin','oxacillin','vancomycin')
    ) AS abx_start_time
  FROM BloquesClasificados b
),

-- (9) Unir icustays
WithICU AS (
  SELECT f.*, icu.intime AS icu_intime, icu.outtime AS icu_outtime, icu.subject_id
  FROM FirstAbx f
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu
    ON f.stay_id = icu.stay_id
  WHERE f.abx_start_time IS NOT NULL
),

-- (10) ItemIDs
ItemLists AS (
  SELECT
    ARRAY<INT64>[220045] AS HR_ITEMIDS,
    ARRAY<INT64>[226329,228242,223762,223761] AS TEMP_ITEMIDS,
    ARRAY<INT64>[220181,220052] AS MAP_ITEMIDS,
    ARRAY<INT64>[229841,229280] AS FIO2_ITEMIDS,
    ARRAY<INT64>[50821] AS PAO2_ITEMIDS,
    ARRAY<INT64>[51300] AS WBC_ITEMIDS,
    ARRAY<INT64>[220948] AS LACTATE_ITEMIDS,
    ARRAY<INT64>[220615,229761] AS CREAT_ITEMIDS,
    ARRAY<INT64>[227457,225170] AS PLATELET_ITEMIDS,
    ARRAY<INT64>[227444] AS CRP_ITEMIDS
),

-- (11) Ventanas diarias
Windows AS (
  SELECT
    w.*,
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP(w.abx_start_time),
      LEAST(TIMESTAMP(w.icu_outtime), TIMESTAMP_ADD(TIMESTAMP(w.abx_start_time), INTERVAL 30 DAY)),
      INTERVAL 1 DAY
    ) AS day_starts
  FROM WithICU w
),

ExplodedWindows AS (
  SELECT w.*, day_start, day_idx
  FROM Windows w, UNNEST(w.day_starts) AS day_start WITH OFFSET AS day_idx
),

-- (12) Agregaciones por ventana
WindowAgg AS (
  SELECT
    ew.bloque,
    ew.subject_id,
    ew.hadm_id,
    ew.stay_id,
    ew.day_idx,
    ew.day_start AS window_start,
    TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY) AS window_end,

    APPROX_QUANTILES(ce_hr.val, 100)[OFFSET(50)] AS hr_med,
    APPROX_QUANTILES(ce_temp.val, 100)[OFFSET(50)] AS temp_med,
    APPROX_QUANTILES(ce_map.val, 100)[OFFSET(50)] AS map_med,
    APPROX_QUANTILES(le_fio2.val, 100)[OFFSET(50)] AS fio2_med,
    APPROX_QUANTILES(le_pao2.val, 100)[OFFSET(50)] AS pao2_med,
    APPROX_QUANTILES(le_wbc.val, 100)[OFFSET(50)] AS wbc_med,
    APPROX_QUANTILES(le_lact.val, 100)[OFFSET(50)] AS lactate_med,
    APPROX_QUANTILES(le_creat.val, 100)[OFFSET(50)] AS creat_med,
    APPROX_QUANTILES(le_platelet.val, 100)[OFFSET(50)] AS platelet_med,
    APPROX_QUANTILES(le_crp.val, 100)[OFFSET(50)] AS crp_med

  FROM ExplodedWindows ew

  -- HR
  LEFT JOIN (
    SELECT stay_id, TIMESTAMP(charttime) AS chart_ts,
           COALESCE(valuenum, SAFE_CAST(value AS FLOAT64)) AS val
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (220045)
  ) ce_hr
  ON ce_hr.stay_id = ew.stay_id
  AND ce_hr.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- TEMP
  LEFT JOIN (
    SELECT stay_id, TIMESTAMP(charttime) AS chart_ts,
           CASE
             WHEN COALESCE(valuenum, SAFE_CAST(value AS FLOAT64)) > 45 THEN 
               (COALESCE(valuenum, SAFE_CAST(value AS FLOAT64)) - 32) * 5/9
             ELSE COALESCE(valuenum, SAFE_CAST(value AS FLOAT64))
           END AS val
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (226329,228242,223762,223761)
  ) ce_temp
  ON ce_temp.stay_id = ew.stay_id
  AND ce_temp.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)


  -- MAP
  LEFT JOIN (
    SELECT stay_id, TIMESTAMP(charttime) AS chart_ts,
           COALESCE(valuenum, SAFE_CAST(value AS FLOAT64)) AS val
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (220181,220052)
  ) ce_map
  ON ce_map.stay_id = ew.stay_id
  AND ce_map.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- FIO2
  LEFT JOIN (
    SELECT stay_id, TIMESTAMP(charttime) AS chart_ts,
           COALESCE(valuenum, SAFE_CAST(value AS FLOAT64)) AS val
    FROM `physionet-data.mimiciv_3_1_icu.chartevents`
    WHERE itemid IN (229841,229280)
  ) le_fio2
  ON le_fio2.stay_id = ew.stay_id
  AND le_fio2.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- PAO2
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid = 50821
  ) le_pao2
  ON le_pao2.hadm_id = ew.hadm_id
  AND le_pao2.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- WBC
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid = 51300
  ) le_wbc
  ON le_wbc.hadm_id = ew.hadm_id
  AND le_wbc.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- Lactate
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid = 220948
  ) le_lact
  ON le_lact.hadm_id = ew.hadm_id
  AND le_lact.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- Creatinine
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid IN (220615,229761)
  ) le_creat
  ON le_creat.hadm_id = ew.hadm_id
  AND le_creat.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- Platelets
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid IN (227457,225170)
  ) le_platelet
  ON le_platelet.hadm_id = ew.hadm_id
  AND le_platelet.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  -- CRP
  LEFT JOIN (
    SELECT hadm_id, TIMESTAMP(charttime) AS chart_ts,
           SAFE_CAST(value AS FLOAT64) AS val
    FROM `physionet-data.mimiciv_3_1_hosp.labevents`
    WHERE itemid = 227444
  ) le_crp
  ON le_crp.hadm_id = ew.hadm_id
  AND le_crp.chart_ts BETWEEN ew.day_start AND TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)

  GROUP BY
    ew.bloque, ew.subject_id, ew.hadm_id, ew.stay_id, ew.day_idx, ew.day_start
)



-- SELECT final
SELECT *
FROM WindowAgg
ORDER BY bloque, subject_id, day_idx;
