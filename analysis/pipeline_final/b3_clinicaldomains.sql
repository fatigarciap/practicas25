-- ============================================
-- 🧩 BLOQUE 3: DOMINIOS CLÍNICOS (SOFA-LIKE) – CORREGIDO
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.clinical_domains` AS
WITH
-- 1️⃣ Cargar datos diarios
daily AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.daily_features`
),

-- 2️⃣ Calcular dominios
domains AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,

    -- ⚙️ Hemodinámico
    CASE
      WHEN MAP_median IS NULL THEN NULL
      WHEN MAP_median < 65 THEN 1 ELSE 0
    END AS hemo_dysfunction,

    CASE
      WHEN Lactate_median IS NULL THEN NULL
      WHEN Lactate_median > 2 THEN 1 ELSE 0
    END AS lactate_high,

    -- ⚙️ Renal
    CASE
      WHEN Creatinine_median IS NULL THEN NULL
      WHEN Creatinine_median >= 2 THEN 1 ELSE 0
    END AS renal_dysfunction,

    -- ⚙️ Respiratorio: SpO₂ / FiO₂ (corrige unidades)
    CASE
      WHEN SpO2_median IS NULL OR FiO2_median IS NULL OR FiO2_median = 0 THEN NULL
      WHEN FiO2_median > 1 THEN SAFE_DIVIDE(SpO2_median, FiO2_median / 100)  -- FiO₂ en %
      ELSE SAFE_DIVIDE(SpO2_median, FiO2_median)                              -- FiO₂ ya fracción
    END AS spo2fio2_ratio,

    CASE
      WHEN SpO2_median IS NULL THEN NULL
      WHEN SpO2_median < 90 THEN 1 ELSE 0
    END AS resp_dysfunction,

    -- ⚙️ Inflamatorio
    CASE
      WHEN WBC_median IS NULL THEN NULL
      WHEN WBC_median < 4 OR WBC_median > 12 THEN 1 ELSE 0
    END AS wbc_abnormal,

    CASE
      WHEN Temp_median IS NULL THEN NULL
      WHEN Temp_median > 38 OR Temp_median < 36 THEN 1 ELSE 0
    END AS temp_abnormal,

    -- ⚙️ Hematológico
    CASE
      WHEN Platelets_median IS NULL THEN NULL
      WHEN Platelets_median < 100 THEN 1 ELSE 0
    END AS hema_dysfunction,

    -- ⚙️ Hepático
    CASE
      WHEN Bilirubin_median IS NULL THEN NULL
      WHEN Bilirubin_median >= 2 THEN 1 ELSE 0
    END AS hepatic_dysfunction

  FROM daily
),

-- 3️⃣ Calcular SOFA-like score
scored AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    SAFE_CAST(
      COALESCE(hemo_dysfunction,0) +
      COALESCE(lactate_high,0) +
      COALESCE(renal_dysfunction,0) +
      COALESCE(resp_dysfunction,0) +
      COALESCE(wbc_abnormal,0) +
      COALESCE(temp_abnormal,0) +
      COALESCE(hema_dysfunction,0) +
      COALESCE(hepatic_dysfunction,0)
      AS INT64
    ) AS sofa_like_score,
    hemo_dysfunction,
    lactate_high,
    renal_dysfunction,
    resp_dysfunction,
    wbc_abnormal,
    temp_abnormal,
    hema_dysfunction,
    hepatic_dysfunction,
    spo2fio2_ratio
  FROM domains
)

-- 4️⃣ Tabla final
SELECT *
FROM scored
ORDER BY subject_id, stay_id, day_idx;
