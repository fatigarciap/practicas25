CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean` AS

WITH t0 AS (
  SELECT *
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
),

rx_all AS (
  SELECT
    t.subject_id,
    t.hadm_id,
    t.stay_id,
    t.microevent_id,
    t.true_t0,
    LOWER(p.drug) AS drug_raw,
    CAST(p.starttime AS TIMESTAMP) AS start_ts,
    CAST(p.stoptime AS TIMESTAMP) AS stop_ts
  FROM t0 t
  JOIN `physionet-data.mimiciv_3_1_hosp.prescriptions` p
    ON t.hadm_id = p.hadm_id
  WHERE p.drug IS NOT NULL
),

rx_active_t0 AS (
  SELECT *
  FROM rx_all
  WHERE start_ts <= true_t0
    AND (
      stop_ts IS NULL
      OR stop_ts > true_t0
    )
),

rx_mapped AS (
  SELECT
    r.*,
    m.match_priority,
    m.abx_name_std,
    m.spectrum_level,
    m.spectrum_label,
    m.coverage_domain
  FROM rx_active_t0 r
  JOIN `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean` m
    ON REGEXP_CONTAINS(r.drug_raw, m.pattern)
),

rx_dedup AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY stay_id, abx_name_std
        ORDER BY start_ts ASC
      ) AS rn
    FROM rx_mapped
  )
  WHERE rn = 1
)

SELECT
  subject_id,
  hadm_id,
  stay_id,
  microevent_id,
  true_t0,
  abx_name_std,
  spectrum_level,
  spectrum_label,
  coverage_domain,
  start_ts,
  stop_ts
FROM rx_dedup;