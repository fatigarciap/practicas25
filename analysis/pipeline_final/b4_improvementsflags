-- ============================================
-- 🧩 BLOQUE 4: IMPROVEMENT FLAGS Y EVENTOS
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.improvement_flags` AS
WITH
-- ------------------------------------------------
-- 1️⃣ Datos base y dominios clínicos
-- ------------------------------------------------
domains AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains`
),
base AS (
  SELECT subject_id, hadm_id, stay_id, t0, day_idx, followup_end
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
),

-- ------------------------------------------------
-- 2️⃣ Combinar y calcular variaciones día a día
-- ------------------------------------------------
joined AS (
  SELECT
    d.subject_id,
    d.hadm_id,
    d.stay_id,
    d.day_idx,
    d.sofa_like_score,
    LAG(d.sofa_like_score) OVER (PARTITION BY d.subject_id, d.stay_id ORDER BY d.day_idx) AS prev_score,
    b.followup_end
  FROM domains d
  LEFT JOIN base b
    USING (subject_id, hadm_id, stay_id)
),

-- ------------------------------------------------
-- 3️⃣ Detectar mejoría fisiológica diaria
-- ------------------------------------------------
flagged AS (
  SELECT
    subject_id,
    hadm_id,
    stay_id,
    day_idx,
    sofa_like_score,
    prev_score,
    CASE
      WHEN prev_score IS NULL THEN NULL  -- día inicial
      WHEN sofa_like_score < prev_score THEN 1
      WHEN sofa_like_score > prev_score THEN 0
      ELSE 0
    END AS improved_today
  FROM joined
),

-- ------------------------------------------------
-- 4️⃣ Mejoría sostenida (≥2 días consecutivos de mejoría o estabilidad)
-- ------------------------------------------------
sustained AS (
  SELECT
    f.subject_id,
    f.hadm_id,
    f.stay_id,
    f.day_idx,
    f.sofa_like_score,
    f.prev_score,
    f.improved_today,
    CASE
      WHEN f.improved_today = 1
       AND LAG(f.improved_today, 1) OVER (PARTITION BY f.subject_id, f.stay_id ORDER BY f.day_idx) = 1
      THEN 1
      ELSE 0
    END AS sustained_improvement
  FROM flagged f
),

-- ------------------------------------------------
-- 5️⃣ Eventos competidores (muerte o alta)
-- ------------------------------------------------
events AS (
  SELECT
    s.*,
    CASE
      WHEN s.day_idx = MAX(s.day_idx) OVER (PARTITION BY s.subject_id, s.stay_id)
      THEN 1 ELSE 0
    END AS discharge_event,
    0 AS death_event  -- placeholder: se puede actualizar si se cruza con admissions.deathtime
  FROM sustained s
)

-- ------------------------------------------------
-- 🧾 Tabla final
-- ------------------------------------------------
SELECT
  subject_id,
  hadm_id,
  stay_id,
  day_idx,
  sofa_like_score,
  prev_score,
  improved_today,
  sustained_improvement,
  discharge_event,
  death_event
FROM events
ORDER BY subject_id, stay_id, day_idx;
