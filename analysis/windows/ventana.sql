-- PLANTILLA: ventanas 24h y agregados por paciente y bloque
-- ------------- Sustituir listas ITEMID & DRUG lists según tu exploración --------------
WITH
-- (1) Tus CTEs iniciales: MicrobiologyEvents, SingleOrganismEvents, ComorbidityExclusions, PriorTreatment, PostTreatment, Filtered, BloquesClasificados
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

SingleOrganismEvents AS (
  SELECT stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  FROM MicrobiologyEvents
  WHERE RowNum = 1
  GROUP BY stay_id, hadm_id, charttime, org_name, antibiotic_class, interpretation
  HAVING COUNT(DISTINCT org_name) = 1
),

ComorbidityExclusions AS (
  SELECT DISTINCT hadm_id
  FROM `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`
  WHERE icd_code LIKE 'N18%' OR icd_code LIKE 'E11%' OR icd_code LIKE 'I50%'
),

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

Filtered AS (
  SELECT * FROM PostTreatment
  WHERE has_prior_treatment = FALSE AND has_post_treatment = TRUE
),

BloquesClasificados AS (
  SELECT *,
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

-- (2) Obtener abx_start_time: la primera prescripción post-cultivo dentro de la ventana 48h
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

-- (3) Unir icustays para obtener icu_outtime y subject
WithICU AS (
  SELECT f.*, icu.intime AS icu_intime, icu.outtime AS icu_outtime, icu.subject_id
  FROM FirstAbx f
  LEFT JOIN `physionet-data.mimiciv_3_1_icu.icustays` icu
    ON f.stay_id = icu.stay_id
  WHERE f.abx_start_time IS NOT NULL
),

-- (4) ITEMIDs: aquí defines arrays de itemid para cada variable.
-- REEMPLAZA estos arrays por los itemid que hayas verificado con d_items.
ItemLists AS (
  SELECT
    ARRAY<INT64>[-- HR itemids to be filled
      220045, 220047, 220046 /* ejemplos */
    ] AS HR_ITEMIDS,
    ARRAY<INT64>[-- Temp
      229236, 224674, 228242,224027,223762,223761,224642,227054/* ejemplo */
    ] AS TEMP_ITEMIDS,
    ARRAY<INT64>[-- MAP
      220179, /* ejemplo */
    ] AS MAP_ITEMIDS,
    ARRAY<INT64>[-- SpO2
      220277 /* ejemplo */
    ] AS SPO2_ITEMIDS,
    ARRAY<INT64>[-- FiO2
      223835 /* ejemplo */
    ] AS FIO2_ITEMIDS,
    ARRAY<INT64>[-- PaO2 (gasometry)
      50821 /* ejemplo de labevents itemid */
    ] AS PAO2_ITEMIDS,
    ARRAY<INT64>[-- WBC
      51300 /* ejemplo */
    ] AS WBC_ITEMIDS,
    ARRAY<INT64>[-- Lactate
      50813 /* ejemplo */
    ] AS LACTATE_ITEMIDS,
    ARRAY<INT64>[-- Creatinine
      50912 /* ejemplo */
    ] AS CREAT_ITEMIDS,
    ARRAY<INT64>[-- Platelets
      51265 /* ejemplo */
    ] AS PLATELET_ITEMIDS,
    ARRAY<INT64>[-- CRP
      51004 /* ejemplo */
    ] AS CRP_ITEMIDS
),

-- (5) Generar ventanas por paciente: array de timestamps [t0, t0+1d, ...]
Windows AS (
  SELECT
    w.*,
    -- generar array de inicios de días, desde abx_start_time hasta icu_outtime (inclusive)
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP(w.abx_start_time),
      LEAST(TIMESTAMP(w.icu_outtime), TIMESTAMP_ADD(TIMESTAMP(w.abx_start_time), INTERVAL 30 DAY)),
      INTERVAL 1 DAY
    ) AS day_starts
  FROM WithICU w
),

ExplodedWindows AS (
  SELECT
    w.*,
    day_start,
    -- day_idx = offset (0,1,2,...)
    OFFSET(day_start, 0) OVER() AS dummy_offset, -- helper, no se usa; la función offset la hacemos con UNNEST
  FROM Windows w, UNNEST(w.day_starts) AS day_start WITH OFFSET AS day_idx
),

-- (6) Agregaciones: por cada ventana buscamos chartevents/labevents en el intervalo [day_start, day_start + 24h)
WindowAgg AS (
  SELECT
    ew.bloque,
    ew.subject_id,
    ew.hadm_id,
    ew.stay_id,
    ew.day_idx,
    ew.day_start AS window_start,
    TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY) AS window_end,

    -- MEDIANAS signos vitales (usamos aproximación con APPROX_QUANTILES)
    (
      SELECT IFNULL( (ARRAY_AGG(AGG ORDER BY AGG)[OFFSET(DIV(ARRAY_LENGTH(AGG),2))]) , NULL)
      FROM (
        SELECT ARRAY_AGG(CAST(val AS FLOAT64) IGNORE NULLS) AGG
        FROM (
          SELECT COALESCE(ce.valuenum, SAFE_CAST(ce.value AS FLOAT64)) AS val
          FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
          WHERE ce.stay_id = ew.stay_id
            AND ce.charttime >= ew.day_start
            AND ce.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
            AND ce.itemid IN UNNEST((SELECT HR_ITEMIDS FROM ItemLists))
        )
      )
    ) AS hr_med,

    -- Usamos APPROX_QUANTILES for temp (ejemplo simplificado)
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT COALESCE(ce.valuenum, SAFE_CAST(ce.value AS FLOAT64)) AS v
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
        WHERE ce.stay_id = ew.stay_id
          AND ce.charttime >= ew.day_start
          AND ce.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND ce.itemid IN UNNEST((SELECT TEMP_ITEMIDS FROM ItemLists))
      )
    ) AS temp_med,

    -- MAP mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT COALESCE(ce.valuenum, SAFE_CAST(ce.value AS FLOAT64)) AS v
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
        WHERE ce.stay_id = ew.stay_id
          AND ce.charttime >= ew.day_start
          AND ce.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND ce.itemid IN UNNEST((SELECT MAP_ITEMIDS FROM ItemLists))
      )
    ) AS map_med,

    -- SpO2 mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT COALESCE(ce.valuenum, SAFE_CAST(ce.value AS FLOAT64)) AS v
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
        WHERE ce.stay_id = ew.stay_id
          AND ce.charttime >= ew.day_start
          AND ce.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND ce.itemid IN UNNEST((SELECT SPO2_ITEMIDS FROM ItemLists))
      )
    ) AS spo2_med,

    -- FiO2 mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT COALESCE(ce.valuenum, SAFE_CAST(ce.value AS FLOAT64)) AS v
        FROM `physionet-data.mimiciv_3_1_icu.chartevents` ce
        WHERE ce.stay_id = ew.stay_id
          AND ce.charttime >= ew.day_start
          AND ce.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND ce.itemid IN UNNEST((SELECT FIO2_ITEMIDS FROM ItemLists))
      )
    ) AS fio2_med,

    -- PaO2 mediana (desde labevents)
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT PAO2_ITEMIDS FROM ItemLists))
      )
    ) AS pao2_med,

    -- WBC mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT WBC_ITEMIDS FROM ItemLists))
      )
    ) AS wbc_med,

    -- Lactate mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT LACTATE_ITEMIDS FROM ItemLists))
      )
    ) AS lactate_med,

    -- Creatinine mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT CREAT_ITEMIDS FROM ItemLists))
      )
    ) AS creat_med,

    -- Platelets mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT PLATELET_ITEMIDS FROM ItemLists))
      )
    ) AS platelet_med,

    -- CRP mediana
    (
      SELECT (APPROX_QUANTILES(CAST(v AS FLOAT64), 100))[OFFSET(50)]
      FROM (
        SELECT SAFE_CAST(le.value AS FLOAT64) AS v
        FROM `physionet-data.mimiciv_3_1_hosp.labevents` le
        WHERE le.hadm_id = ew.hadm_id
          AND le.charttime >= ew.day_start
          AND le.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
          AND le.itemid IN UNNEST((SELECT CRP_ITEMIDS FROM ItemLists))
      )
    ) AS crp_med,

    -- abx_active: existe prescripción que cubre la ventana
    EXISTS (
      SELECT 1 FROM `physionet-data.mimiciv_3_1_hosp.prescriptions` pr
      WHERE pr.hadm_id = ew.hadm_id
        AND pr.starttime <= TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
        AND (pr.stoptime IS NULL OR pr.stoptime >= ew.day_start)
        AND LOWER(pr.drug) IN ('ceftriaxone','cefepime','ceftazidime','meropenem','imipenem','trimethoprim-sulfamethoxazole','linezolid','daptomycin','oxacillin','vancomycin')
    ) AS abx_active,

    -- vasopressor_use: existencia de fármaco vasoactivo en la ventana
    EXISTS (
      SELECT 1 FROM `physionet-data.mimiciv_3_1_hosp.prescriptions` pr
      WHERE pr.hadm_id = ew.hadm_id
        AND pr.starttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
        AND (pr.stoptime IS NULL OR pr.stoptime >= ew.day_start)
        AND LOWER(pr.drug) IN ('norepinephrine','vasopressin','epinephrine','dobutamine','dopamine')
    ) AS vasopressor_use,

    -- death_event: paciente muere en esa ventana (revisar patients.deathtime / admissions)
    EXISTS (
      SELECT 1 FROM `physionet-data.mimiciv_3_1_core.patients` pt
      WHERE pt.subject_id = ew.subject_id
        AND pt.deathtime IS NOT NULL
        AND pt.deathtime >= ew.day_start
        AND pt.deathtime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
    ) AS death_event,

    -- new_culture: hay nuevo micro evento positivo en la ventana (puedes afinar por mismo germen/foco)
    EXISTS (
      SELECT 1 FROM `physionet-data.mimiciv_3_1_hosp.microbiologyevents` me
      WHERE me.hadm_id = ew.hadm_id
        AND me.charttime >= ew.day_start
        AND me.charttime < TIMESTAMP_ADD(ew.day_start, INTERVAL 1 DAY)
        -- si quieres detectar "nuevo foco distinto", añadir AND LOWER(me.org_name) != LOWER(ew.org_name)
    ) AS new_culture

  FROM ExplodedWindows ew
)

-- (7) Calcular improved usando reglas simplificadas
SELECT
  wa.bloque,
  wa.subject_id,
  wa.hadm_id,
  wa.stay_id,
  wa.day_idx,
  wa.window_start,
  wa.window_end,
  wa.hr_med,
  wa.temp_med,
  wa.wbc_med,
  wa.lactate_med,
  wa.map_med,
  wa.pao2_med,
  wa.abx_active,
  wa.vasopressor_use,
  wa.death_event,
  wa.new_culture,

  -- Reglas simplificadas: cada criterio da 1 si cumple, 0 si no (puedes afinar thresholds)
  CASE WHEN wa.temp_med IS NULL THEN 0 WHEN wa.temp_med BETWEEN 36 AND 38 THEN 1
       ELSE 0 END AS temp_ok,
  CASE WHEN wa.wbc_med IS NULL THEN 0 WHEN wa.wbc_med BETWEEN 4 AND 12 THEN 1 ELSE 0 END AS wbc_ok,
  CASE WHEN wa.map_med IS NULL THEN 0 WHEN wa.map_med >= 65 AND wa.vasopressor_use = FALSE THEN 1 ELSE 0 END AS hemo_ok,
  CASE WHEN wa.lactate_med IS NULL THEN 0 WHEN wa.lactate_med < 2 THEN 1 ELSE 0 END AS lactate_ok,
  CASE WHEN wa.pao2_med IS NULL OR wa.fio2_med IS NULL THEN 0
       WHEN SAFE_DIVIDE(wa.pao2_med, NULLIF(wa.fio2_med,0)) > 240 THEN 1 ELSE 0 END AS resp_ok,

  -- improved summary: al menos 3 criterios positivos y sin eventos negativos
  CASE
    WHEN ( (CASE WHEN wa.temp_med BETWEEN 36 AND 38 THEN 1 ELSE 0 END)
         + (CASE WHEN wa.wbc_med BETWEEN 4 AND 12 THEN 1 ELSE 0 END)
         + (CASE WHEN wa.map_med >= 65 AND wa.vasopressor_use = FALSE THEN 1 ELSE 0 END)
         + (CASE WHEN wa.lactate_med IS NOT NULL AND wa.lactate_med < 2 THEN 1 ELSE 0 END)
         + (CASE WHEN wa.pao2_med IS NOT NULL AND wa.fio2_med IS NOT NULL AND SAFE_DIVIDE(wa.pao2_med, NULLIF(wa.fio2_med,0)) > 240 THEN 1 ELSE 0 END)
        ) >= 3
      AND wa.death_event = FALSE
      AND wa.new_culture = FALSE
  THEN 1 ELSE 0 END AS improved

FROM WindowAgg wa
ORDER BY bloque, subject_id, day_idx;
