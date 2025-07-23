SELECT
  COUNT(DISTINCT icu.stay_id) AS total_estancias_uci_cultivos_positivos
FROM
  `physionet-data.mimiciv_3_1_icu.icustays` icu
INNER JOIN
  `physionet-data.mimiciv_3_1_hosp.microbiologyevents` micro
ON
  icu.hadm_id = micro.hadm_id
WHERE
  micro.hadm_id IS NOT NULL
  AND micro.charttime IS NOT NULL
  AND micro.org_name IS NOT NULL
  AND (
    LOWER(micro.org_name) LIKE '%escherichia coli%'
    OR LOWER(micro.org_name) LIKE '%klebsiella pneumoniae%'
    OR LOWER(micro.org_name) LIKE '%klebsiella aerogenes%'
    OR LOWER(micro.org_name) LIKE '%enterobacter cloacae%'
    OR LOWER(micro.org_name) LIKE '%enterobacter aerogenes%'
    OR LOWER(micro.org_name) LIKE '%pseudomonas aeruginosa%'
    OR LOWER(micro.org_name) LIKE '%acinetobacter baumannii%'
    OR LOWER(micro.org_name) LIKE '%stenotrophomonas maltophilia%'
    OR LOWER(micro.org_name) LIKE '%enterococcus faecium%'
    OR LOWER(micro.org_name) LIKE '%staphylococcus aureus%'
  )
