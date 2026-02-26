CREATE OR REPLACE TABLE
  `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_clean` AS

WITH
-- 1️⃣ Ventanas base (eje temporal)
base AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    window_start,
    window_end
  FROM `strange-math-456415-c3.mimic_analysis.base_windows`
),

-- 2️⃣ Variables clínicas diarias
features AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    HR_median,
    MAP_median,
    Temp_median,
    SpO2_median,
    FiO2_median,
    spo2fio2_ratio,
    WBC_median,
    Lactate_median
  FROM `strange-math-456415-c3.mimic_analysis.daily_features`
),

-- 3️⃣ Flags de mejoría (S-CI)
flags AS (
  SELECT
    hadm_id,
    stay_id,
    day_idx,
    no_new_foci_flag,
    n_domains_ok,
    improved_today,
    sustained_improvement
  FROM `strange-math-456415-c3.mimic_analysis.improvement_flags`
)

-- 4️⃣ Unión final
SELECT
  b.hadm_id,
  b.stay_id,
  b.day_idx,
  b.window_start,
  b.window_end,

  -- Variables clínicas
  f.HR_median,
  f.MAP_median,
  f.Temp_median,
  f.SpO2_median,
  f.FiO2_median,
  f.spo2fio2_ratio,
  f.WBC_median,
  f.Lactate_median,

  -- Evolución clínica
  fl.no_new_foci_flag,
  fl.n_domains_ok,
  fl.improved_today,
  fl.sustained_improvement

FROM base b
LEFT JOIN features f
  USING (hadm_id, stay_id, day_idx)
LEFT JOIN flags fl
  USING (hadm_id, stay_id, day_idx)

ORDER BY stay_id, day_idx;
