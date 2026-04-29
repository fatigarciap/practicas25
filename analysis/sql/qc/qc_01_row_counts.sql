-- proposito:
--   Contar filas en cada tabla intermedia y final del pipeline auditado.
--
-- tablas usadas:
--   - `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_0_antibiogram_detail_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
--   - `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
--   - `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
--   - `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
--   - `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
--   - `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
--   - `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean`
--   - `strange-math-456415-c3.mimic_analysis.radiology_flag_clean`
--   - `strange-math-456415-c3.mimic_analysis.new_foci_events_clean`
--   - `strange-math-456415-c3.mimic_analysis.new_foci_flag_clean`
--   - `strange-math-456415-c3.mimic_analysis.daily_features_clean`
--   - `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
--   - `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
--   - `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
--
-- nivel de agregacion:
--   - Una fila por tabla.
--
-- claves esperadas:
--   - table_name.
--
-- posibles errores detectables:
--   - Tablas vacias.
--   - Perdidas inesperadas entre etapas.
--   - Expansion diaria ausente o excesiva.

SELECT 'bloque_0_episode_candidates_clean' AS table_name, COUNT(*) AS n_rows FROM `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean`
UNION ALL SELECT 'bloque_0_antibiogram_detail_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.bloque_0_antibiogram_detail_clean`
UNION ALL SELECT 'bloque_0b_index_stay_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean`
UNION ALL SELECT 'abx_spectrum_map_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean`
UNION ALL SELECT 'bloque_t0_true', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.bloque_t0_true`
UNION ALL SELECT 'baseline_regimen_detail_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean`
UNION ALL SELECT 'baseline_regimen_summary_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean`
UNION ALL SELECT 'baseline_regimen_multihot_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean`
UNION ALL SELECT 'bloque_1_base_windows_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean`
UNION ALL SELECT 'radiology_worsening_events_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean`
UNION ALL SELECT 'radiology_flag_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.radiology_flag_clean`
UNION ALL SELECT 'new_foci_events_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.new_foci_events_clean`
UNION ALL SELECT 'new_foci_flag_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.new_foci_flag_clean`
UNION ALL SELECT 'daily_features_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.daily_features_clean`
UNION ALL SELECT 'clinical_domains_sci_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean`
UNION ALL SELECT 'improvement_flags_clean', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.improvement_flags_clean`
UNION ALL SELECT 'longitudinal_cohort_model_ready', COUNT(*) FROM `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready`
ORDER BY table_name;
