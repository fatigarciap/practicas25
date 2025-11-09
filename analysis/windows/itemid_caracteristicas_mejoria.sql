-- Heart Rate
SELECT itemid, label
FROM `physionet-data.mimiciv_3_1_icu.d_items`
WHERE LOWER(label) LIKE '%heart rate%'
ORDER BY label
LIMIT 100;

-- Temperatura
SELECT itemid, label
FROM `physionet-data.mimiciv_3_1_icu.d_items`
WHERE LOWER(label) LIKE '%temperature%'
ORDER BY label
LIMIT 100;

-- MAP
SELECT itemid, label
FROM `physionet-data.mimiciv_3_1_icu.d_items`
WHERE LOWER(label) LIKE '%mean arterial pressure%'
ORDER BY label
LIMIT 100;

-- SpO2
SELECT itemid, label
FROM `physionet-data.mimiciv_3_1_icu.d_items`
WHERE LOWER(label) LIKE '%spo2%'
ORDER BY label
LIMIT 100;

-- FiO2
SELECT itemid, label
FROM `physionet-data.mimiciv_3_1_icu.d_items`
WHERE LOWER(label) LIKE '%fio2%'
ORDER BY label
LIMIT 100;
