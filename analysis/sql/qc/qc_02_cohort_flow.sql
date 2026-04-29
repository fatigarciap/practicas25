-- proposito:
--   Describir el flujo de cohorte desde candidatos microbiologicos hasta tabla final.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
--   - `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
--   - `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
--
-- nivel de agregacion:
--   - Una fila por etapa del pipeline.
--
-- claves esperadas:
--   - subject_id, hadm_id, stay_id, microevent_id.
--
-- posibles errores detectables:
--   - Perdida de estancias al seleccionar episodio indice.
--   - Perdida de estancias sin t0 antibiotico.
--   - Ausencia de expansion longitudinal.
--   - Duplicacion no esperada de `microevent_id` tras la tabla indice.

WITH flow AS (
  SELECT
    '00_episode_candidates' AS stage,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT subject_id) AS n_subjects,
    COUNT(DISTINCT hadm_id) AS n_hadm,
    COUNT(DISTINCT stay_id) AS n_stays,
    COUNT(DISTINCT microevent_id) AS n_microevents
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean`

  UNION ALL
  SELECT
    '00b_index_stay',
    COUNT(*),
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT hadm_id),
    COUNT(DISTINCT stay_id),
    COUNT(DISTINCT microevent_id)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`

  UNION ALL
  SELECT
    '01_t0_true',
    COUNT(*),
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT hadm_id),
    COUNT(DISTINCT stay_id),
    COUNT(DISTINCT microevent_id)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`

  UNION ALL
  SELECT
    '02_base_windows',
    COUNT(*),
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT hadm_id),
    COUNT(DISTINCT stay_id),
    COUNT(DISTINCT microevent_id)
  FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`

  UNION ALL
  SELECT
    '06_final_table',
    COUNT(*),
    COUNT(DISTINCT subject_id),
    COUNT(DISTINCT hadm_id),
    COUNT(DISTINCT stay_id),
    COUNT(DISTINCT microevent_id)
  FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
)

SELECT *
FROM flow
ORDER BY stage;
