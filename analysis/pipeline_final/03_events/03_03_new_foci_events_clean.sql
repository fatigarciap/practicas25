CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.new_foci_events_clean` AS

WITH baseline AS (

  SELECT
    stay_id,
    hadm_id,
    t0,
    LOWER(organism_name) AS baseline_org_name,
    LOWER(specimen_type) AS baseline_spec_type
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
  WHERE day_idx = 0
),

followup_micro AS (

  SELECT
    b.stay_id,
    b.hadm_id,
    CAST(m.charttime AS TIMESTAMP) AS charttime,
    LOWER(m.org_name) AS org_name,
    LOWER(m.spec_type_desc) AS spec_type_desc
  FROM baseline b
  JOIN `physionet-data.mimiciv_3_1_hosp.microbiologyevents` m
    ON b.hadm_id = m.hadm_id
  WHERE m.charttime IS NOT NULL
    AND CAST(m.charttime AS TIMESTAMP) > b.t0
    AND m.org_name IS NOT NULL
),

classified AS (

  SELECT
    f.*,
    CASE
      WHEN f.spec_type_desc LIKE '%blood%' THEN 1
      WHEN f.spec_type_desc != b.baseline_spec_type THEN 1
      WHEN f.org_name != b.baseline_org_name THEN 1
      ELSE 0
    END AS new_focus_flag
  FROM followup_micro f
  JOIN baseline b
    USING (stay_id)
)

SELECT
  stay_id,
  hadm_id,
  charttime,
  1 AS new_focus_flag
FROM classified
WHERE new_focus_flag = 1;