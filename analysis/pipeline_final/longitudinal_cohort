-- ============================================
-- 🧩 BLOQUE 5: TABLA LONGITUDINAL FINAL
-- ============================================

CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.longitudinal_cohort` AS
WITH
-- ------------------------------------------------
-- 1️⃣ Cargar cada bloque previo
-- ------------------------------------------------
base AS (
  SELECT subject_id, hadm_id, stay_id, day_idx, t0, window_start, window_end, followup_end
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
),
features AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.daily_features`
),
domains AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.clinical_domains`
),
flags AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags`
),

-- ------------------------------------------------
-- 2️⃣ Integrar tablas (por subject_id, stay_id, day_idx)
-- ------------------------------------------------
joined AS (
  SELECT
    b.subject_id,
    b.hadm_id,
    b.stay_id,
    b.day_idx,
    b.t0,
    b.window_start,
    b.window_end,
    b.followup_end,

    -- 🩸 Vitales y laboratorios (bloque 2)
    f.HR_median,
    f.MAP_median,
    f.Temp_median,
    f.RR_median,
    f.SpO2_median,
    f.FiO2_median,
    f.WBC_median,
    f.Lactate_median,
    f.Creatinine_median,
    f.Bilirubin_median,
    f.Platelets_median,
    f.CRP_median,
    f.Hgb_median,

    -- ⚙️ Dominios clínicos (bloque 3)
    d.sofa_like_score,
    d.hemo_dysfunction,
    d.lactate_high,
    d.renal_dysfunction,
    d.resp_dysfunction,
    d.wbc_abnormal,
    d.temp_abnormal,
    d.hema_dysfunction,
    d.hepatic_dysfunction,
    d.spo2fio2_ratio,

    -- 🚦 Flags de evolución (bloque 4)
    fl.improved_today,
    fl.sustained_improvement,
    fl.discharge_event,
    fl.death_event

  FROM base b
  LEFT JOIN features f
    USING (subject_id, hadm_id, stay_id, day_idx)
  LEFT JOIN domains d
    USING (subject_id, hadm_id, stay_id, day_idx)
  LEFT JOIN flags fl
    USING (subject_id, hadm_id, stay_id, day_idx)
)

-- ------------------------------------------------
-- 🧾 Tabla final longitudinal
-- ------------------------------------------------
SELECT
  subject_id,
  hadm_id,
  stay_id,
  day_idx,
  t0,
  window_start,
  window_end,
  followup_end,
  HR_median,
  MAP_median,
  Temp_median,
  RR_median,
  SpO2_median,
  FiO2_median,
  WBC_median,
  Lactate_median,
  Creatinine_median,
  Bilirubin_median,
  Platelets_median,
  CRP_median,
  Hgb_median,
  sofa_like_score,
  hemo_dysfunction,
  lactate_high,
  renal_dysfunction,
  resp_dysfunction,
  wbc_abnormal,
  temp_abnormal,
  hema_dysfunction,
  hepatic_dysfunction,
  spo2fio2_ratio,
  improved_today,
  sustained_improvement,
  discharge_event,
  death_event
FROM joined
ORDER BY subject_id, stay_id, day_idx;
