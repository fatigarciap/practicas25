-- ============================================
-- 🧩 BLOQUE 0: COHORTE BASE + CULTIVO ÍNDICE + T0
--   - Cultivo válido (patógenos interés + muestras excluidas)
--   - Excluye polimicrobianas (microevent_id con >1 org)
--   - Asigna cultivo a estancia UCI (por tiempo, con margen)
--   - Define cultivo índice = primer cultivo elegible del ingreso (hadm_id)
--   - Define T0 = primer antibiótico en 48h post-cultivo
--   - Excluye antibiótico en 48h pre-cultivo
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis_us.bloque_0_cohorte` AS

WITH
-- ----------------------------
-- 0) Parámetros (ajustables)
-- ----------------------------
params AS (
  SELECT
    24 AS margin_hours_before_icu,   -- margen: aceptar cultivos hasta 24h antes de UCI
    48 AS abx_window_hours           -- ventana: antibióticos 48h antes/después del cultivo
),

-- ----------------------------
-- 1) Estancias UCI
-- ----------------------------
icu_stays AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    intime,
    outtime
  FROM `physionet-data.mimiciv_3_1_icu.icustays`
  WHERE hadm_id IS NOT NULL
),

-- ----------------------------
-- 2) Microbiología filtrada (patógenos + muestras válidas)
-- ----------------------------
micro_filtered AS (
  SELECT
    micro.subject_id,
    micro.hadm_id,
    micro.microevent_id,
    micro.charttime,
    LOWER(micro.org_name) AS org_name,
    LOWER(micro.spec_type_desc) AS spec_type_desc
  FROM `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
  WHERE
    micro.hadm_id IS NOT NULL
    AND micro.charttime IS NOT NULL
    AND micro.org_name IS NOT NULL
    AND micro.interpretation IN ('R','S','I')
    AND LOWER(micro.org_name) IN (
      'escherichia coli',
      'klebsiella pneumoniae',
      'klebsiella aerogenes',
      'enterobacter cloacae',
      'enterobacter aerogenes',
      'pseudomonas aeruginosa',
      'acinetobacter baumannii',
      'stenotrophomonas maltophilia',
      'enterococcus faecium',
      'staphylococcus aureus'
    )
    AND LOWER(micro.spec_type_desc) NOT IN (
      'swab',
      'fluid,other',
      'foreign body',
      'foot culture',
      'fluid received in blood culture bottles',
      'ear',
      'fluid wound',
      'dialysis fluid',
      'skin scrapings',
      'foreign body - sonication culture',
      'eye'
    )
),

-- ----------------------------
-- 3) Excluir polimicrobianas (microevent_id con >1 org)
-- ----------------------------
micro_monomicrobial AS (
  SELECT mf.*
  FROM micro_filtered mf
  JOIN (
    SELECT
      microevent_id,
      COUNT(DISTINCT org_name) AS n_orgs
    FROM micro_filtered
    GROUP BY microevent_id
  ) c
  ON mf.microevent_id = c.microevent_id
  WHERE c.n_orgs = 1
),

-- ----------------------------
-- 4) Asignar cultivo a estancia UCI por ventana temporal
--    (evita duplicar por múltiples stay_id del mismo hadm_id)
-- ----------------------------
micro_assigned_to_icu AS (
  SELECT
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    m.microevent_id,
    m.charttime AS culture_time,
    m.org_name,
    m.spec_type_desc,
    i.intime,
    i.outtime
  FROM micro_monomicrobial m
  JOIN icu_stays i
    ON m.hadm_id = i.hadm_id
  CROSS JOIN params p
  WHERE
    -- Cultivo dentro de UCI o hasta X horas antes del ingreso en UCI (margen clínico)
    m.charttime BETWEEN TIMESTAMP_SUB(i.intime, INTERVAL p.margin_hours_before_icu HOUR)
                   AND i.outtime
),

-- ----------------------------
-- 5) Definir CULTIVO ÍNDICE por ingreso:
--    el primer cultivo elegible (hadm_id) que esté asociado a alguna estancia UCI
-- ----------------------------
index_culture AS (
  SELECT *
  FROM micro_assigned_to_icu
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY hadm_id
    ORDER BY culture_time
  ) = 1
),

-- ----------------------------
-- 6) Prescripciones antibióticas (fuente: prescriptions)
--    Marcamos si un fármaco es "antibiótico relevante" (matching robusto)
-- ----------------------------
abx_prescriptions AS (
  SELECT
    hadm_id,
    starttime,
    stoptime,
    LOWER(drug) AS drug_l
  FROM `physionet-data.mimiciv_3_1_hosp.prescriptions`
  WHERE hadm_id IS NOT NULL AND starttime IS NOT NULL
),

abx_flagged AS (
  SELECT
    a.*,
    (
      REGEXP_CONTAINS(drug_l, r'\bamoxicillin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bampicillin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bpenicillin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bcefazolin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bamoxicillin.*clav') OR
      REGEXP_CONTAINS(drug_l, r'\bcefuroxime\b') OR
      REGEXP_CONTAINS(drug_l, r'\bceftriaxone\b') OR
      REGEXP_CONTAINS(drug_l, r'\bcefotaxime\b') OR
      REGEXP_CONTAINS(drug_l, r'\bpiperacillin\b') OR
      REGEXP_CONTAINS(drug_l, r'\btazobactam\b') OR
      REGEXP_CONTAINS(drug_l, r'\bcefepime\b') OR
      REGEXP_CONTAINS(drug_l, r'\blevofloxacin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bciprofloxacin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bmeropenem\b') OR
      REGEXP_CONTAINS(drug_l, r'\bimipenem\b') OR
      REGEXP_CONTAINS(drug_l, r'\bvaborbactam\b') OR
      REGEXP_CONTAINS(drug_l, r'\bavibactam\b') OR
      REGEXP_CONTAINS(drug_l, r'\bvancomycin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bteicoplanin\b') OR
      REGEXP_CONTAINS(drug_l, r'\blinezolid\b') OR
      REGEXP_CONTAINS(drug_l, r'\bdaptomycin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bceftaroline\b') OR
      REGEXP_CONTAINS(drug_l, r'\bcolistin\b') OR
      REGEXP_CONTAINS(drug_l, r'\btigecycline\b') OR
      REGEXP_CONTAINS(drug_l, r'\bfosfomycin\b') OR
      REGEXP_CONTAINS(drug_l, r'\bcefiderocol\b') OR
      REGEXP_CONTAINS(drug_l, r'\btrimethoprim\b') OR
      REGEXP_CONTAINS(drug_l, r'\bsulfamethoxazole\b')
    ) AS is_relevant_abx
  FROM abx_prescriptions a
),

-- ----------------------------
-- 7) Exposición antibiótica previa (48h antes del cultivo)
-- ----------------------------
prior_abx AS (
  SELECT
    ic.hadm_id,
    ic.stay_id,
    ic.microevent_id,
    ic.culture_time,
    COUNTIF(ab.is_relevant_abx) > 0 AS has_prior_abx_48h
  FROM index_culture ic
  CROSS JOIN params p
  LEFT JOIN abx_flagged ab
    ON ic.hadm_id = ab.hadm_id
    AND ab.starttime >= TIMESTAMP_SUB(ic.culture_time, INTERVAL p.abx_window_hours HOUR)
    AND ab.starttime <  ic.culture_time
  GROUP BY ic.hadm_id, ic.stay_id, ic.microevent_id, ic.culture_time
),

-- ----------------------------
-- 8) Definir T0 (primer antibiótico relevante en 48h post-cultivo)
-- ----------------------------
t0_defined AS (
  SELECT
    ic.*,
    MIN(ab.starttime) AS t0_starttime
  FROM index_culture ic
  CROSS JOIN params p
  LEFT JOIN abx_flagged ab
    ON ic.hadm_id = ab.hadm_id
    AND ab.is_relevant_abx
    AND ab.starttime >= ic.culture_time
    AND ab.starttime <= TIMESTAMP_ADD(ic.culture_time, INTERVAL p.abx_window_hours HOUR)
  GROUP BY
    ic.subject_id, ic.hadm_id, ic.stay_id,
    ic.microevent_id, ic.culture_time,
    ic.org_name, ic.spec_type_desc, ic.intime, ic.outtime
),

-- ----------------------------
-- 9) Cohorte final Bloque 0 (sin ABX previo + con T0)
-- ----------------------------
final_cohort AS (
  SELECT
    t.subject_id,
    t.hadm_id,
    t.stay_id,
    t.microevent_id,
    t.culture_time,
    t.t0_starttime AS t0,
    t.org_name,
    t.spec_type_desc,
    t.intime,
    t.outtime
  FROM t0_defined t
  JOIN prior_abx p
    ON t.hadm_id = p.hadm_id
    AND t.stay_id = p.stay_id
    AND t.microevent_id = p.microevent_id
    AND t.culture_time = p.culture_time
  WHERE
    p.has_prior_abx_48h = FALSE
    AND t.t0_starttime IS NOT NULL
)

SELECT *
FROM final_cohort;
