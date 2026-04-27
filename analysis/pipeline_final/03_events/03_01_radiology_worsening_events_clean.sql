CREATE OR REPLACE TABLE
`strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean` AS

WITH cohort AS (
SELECT DISTINCT
    stay_id,
    hadm_id,
    t0
FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
),

radiology_notes AS (
SELECT
    r.note_id,
    r.subject_id,
    r.hadm_id,
    CAST(r.charttime AS TIMESTAMP) AS charttime,
    LOWER(r.text) AS note_text
FROM `physionet-data.mimiciv_note.radiology` r
WHERE r.text IS NOT NULL
AND r.charttime IS NOT NULL
),

worsening_positive AS (
SELECT
    c.stay_id,
    c.hadm_id,
    r.charttime,
    r.note_id,
    r.note_text
FROM cohort c
JOIN radiology_notes r
ON c.hadm_id = r.hadm_id
WHERE r.charttime > c.t0
AND (

REGEXP_CONTAINS(r.note_text, r'new consolidation')
OR REGEXP_CONTAINS(r.note_text, r'new infiltrate')
OR REGEXP_CONTAINS(r.note_text, r'new opacity')
OR REGEXP_CONTAINS(r.note_text, r'new pleural effusion')
OR REGEXP_CONTAINS(r.note_text, r'worsening infiltrate')
OR REGEXP_CONTAINS(r.note_text, r'worsening consolidation')
OR REGEXP_CONTAINS(r.note_text, r'progression of infiltrates')
OR REGEXP_CONTAINS(r.note_text, r'progression of consolidation')
OR REGEXP_CONTAINS(r.note_text, r'worsening appearance')
OR REGEXP_CONTAINS(r.note_text, r'increased compared to prior')

)
),

worsening_filtered AS (
SELECT *
FROM worsening_positive
WHERE NOT (
REGEXP_CONTAINS(note_text, r'improved')
OR REGEXP_CONTAINS(note_text, r'improvement')
OR REGEXP_CONTAINS(note_text, r'stable')
OR REGEXP_CONTAINS(note_text, r'no significant change')
OR REGEXP_CONTAINS(note_text, r'unchanged')
)
)

SELECT DISTINCT
stay_id,
hadm_id,
charttime,
1 AS radiology_worsening_flag
FROM worsening_filtered;