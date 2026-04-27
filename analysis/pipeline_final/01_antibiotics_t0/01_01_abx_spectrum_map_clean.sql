CREATE OR REPLACE TABLE `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean` AS

SELECT 1 AS match_priority, r'ampicillin.*sulbactam' AS pattern, 'ampicillin_sulbactam' AS abx_name_std, 2 AS spectrum_level, 'intermedio' AS spectrum_label, 'general' AS coverage_domain UNION ALL
SELECT 2, r'amoxicillin.*clav', 'amoxicillin_clavulanate', 2, 'intermedio', 'general' UNION ALL
SELECT 3, r'piperacillin.*tazobactam', 'pip_tazo', 3, 'amplio', 'general' UNION ALL
SELECT 4, r'ceftazidime.*avibactam', 'ceftazidime_avibactam', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 5, r'ceftolozane.*tazobactam', 'ceftolozane_tazobactam', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 6, r'meropenem.*vaborbactam', 'meropenem_vaborbactam', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL

SELECT 10, r'cefazolin', 'cefazolin', 1, 'estrecho', 'general' UNION ALL
SELECT 11, r'penicillin', 'penicillin', 1, 'estrecho', 'general' UNION ALL
SELECT 12, r'amoxicillin', 'amoxicillin', 1, 'estrecho', 'general' UNION ALL
SELECT 13, r'ampicillin', 'ampicillin', 1, 'estrecho', 'general' UNION ALL

SELECT 20, r'cefuroxime', 'cefuroxime', 2, 'intermedio', 'general' UNION ALL
SELECT 21, r'ceftriaxone', 'ceftriaxone', 2, 'intermedio', 'general' UNION ALL
SELECT 22, r'cefotaxime', 'cefotaxime', 2, 'intermedio', 'general' UNION ALL
SELECT 23, r'metronidazole', 'metronidazole', 2, 'intermedio', 'anaerobios' UNION ALL
SELECT 24, r'clindamycin', 'clindamycin', 2, 'intermedio', 'general' UNION ALL
SELECT 25, r'trimethoprim.*sulfamethoxazole|co-trimoxazole', 'tmp_smx', 2, 'intermedio', 'general' UNION ALL

SELECT 30, r'cefepime', 'cefepime', 3, 'amplio', 'general' UNION ALL
SELECT 31, r'ciprofloxacin', 'ciprofloxacin', 3, 'amplio', 'general' UNION ALL
SELECT 32, r'levofloxacin', 'levofloxacin', 3, 'amplio', 'general' UNION ALL
SELECT 33, r'aztreonam', 'aztreonam', 3, 'amplio', 'general' UNION ALL
SELECT 34, r'gentamicin', 'gentamicin', 3, 'amplio', 'general' UNION ALL
SELECT 35, r'tobramycin', 'tobramycin', 3, 'amplio', 'general' UNION ALL

SELECT 40, r'meropenem', 'meropenem', 4, 'muy_amplio', 'general_reserva' UNION ALL
SELECT 41, r'imipenem', 'imipenem', 4, 'muy_amplio', 'general_reserva' UNION ALL
SELECT 42, r'ertapenem', 'ertapenem', 4, 'muy_amplio', 'general_reserva' UNION ALL
SELECT 43, r'amikacin', 'amikacin', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 44, r'colistin', 'colistin', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 45, r'polymyxin b', 'polymyxin_b', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 46, r'tigecycline', 'tigecycline', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 47, r'cefiderocol', 'cefiderocol', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL
SELECT 48, r'fosfomycin', 'fosfomycin_iv', 4, 'muy_amplio', 'gn_multirresistente' UNION ALL

SELECT 50, r'vancomycin', 'vancomycin_iv', 4, 'muy_amplio', 'gp_resistente' UNION ALL
SELECT 51, r'teicoplanin', 'teicoplanin', 4, 'muy_amplio', 'gp_resistente' UNION ALL
SELECT 52, r'linezolid', 'linezolid', 4, 'muy_amplio', 'gp_resistente' UNION ALL
SELECT 53, r'daptomycin', 'daptomycin', 4, 'muy_amplio', 'gp_resistente' UNION ALL
SELECT 54, r'ceftaroline', 'ceftaroline', 4, 'muy_amplio', 'gp_resistente';