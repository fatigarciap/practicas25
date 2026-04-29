# PIPELINE AUDIT

Fecha de auditoria: 2026-04-28  
Alcance: scripts SQL bajo `analysis/`. No se ejecutaron consultas contra BigQuery; el mapa se deriva exclusivamente del codigo SQL disponible en el repositorio.

## 1. Inventario completo de SQL

| Script | Tabla creada | Rol observado |
|---|---|---|
| `analysis/pipeline_final/00_cohort/00_01_episode_candidates_clean.sql` | `strange-math-456415-c3.mimic_analysis.bloque_0_episode_candidates_clean` | Candidatos de episodio microbiologico en UCI |
| `analysis/pipeline_final/00_cohort/00_02_antibiogram_detail_clean.sql` | `strange-math-456415-c3.mimic_analysis.bloque_0_antibiogram_detail_clean` | Detalle de antibiograma asociado al evento indice |
| `analysis/pipeline_final/00_cohort/00_03_index_stay_clean.sql` | `strange-math-456415-c3.mimic_analysis.bloque_0b_index_stay_clean` | Seleccion de un episodio indice por estancia |
| `analysis/pipeline_final/01_antibiotics_t0/01_01_abx_spectrum_map_clean.sql` | `strange-math-456415-c3.mimic_analysis.abx_spectrum_map_clean` | Mapa manual de antibioticos y espectro |
| `analysis/pipeline_final/01_antibiotics_t0/01_01_t0_true.sql` | `strange-math-456415-c3.mimic_analysis.bloque_t0_true` | Definicion de t0 terapeutico |
| `analysis/pipeline_final/01_antibiotics_t0/01_03_baseline_regimen_detail_clean.sql` | `strange-math-456415-c3.mimic_analysis.baseline_regimen_detail_clean` | Antibioticos activos en t0 |
| `analysis/pipeline_final/01_antibiotics_t0/01_04_baseline_regimen_summary_clean.sql` | `strange-math-456415-c3.mimic_analysis.baseline_regimen_summary_clean` | Resumen de regimen basal |
| `analysis/pipeline_final/01_antibiotics_t0/01_05_baseline_regimen_multihot_clean.sql` | `strange-math-456415-c3.mimic_analysis.baseline_regimen_multihot_clean` | Variables multihot del regimen basal |
| `analysis/pipeline_final/02_windows/02_01_base_windows_clean.sql` | `strange-math-456415-c3.mimic_analysis.bloque_1_base_windows_clean` | Ventanas longitudinales desde t0 |
| `analysis/pipeline_final/03_events/03_01_radiology_worsening_events_clean.sql` | `strange-math-456415-c3.mimic_analysis.radiology_worsening_events_clean` | Eventos de empeoramiento radiologico por regex |
| `analysis/pipeline_final/03_events/03_02_radiology_flag_clean.sql` | `strange-math-456415-c3.mimic_analysis.radiology_flag_clean` | Flag diario de estabilidad radiologica |
| `analysis/pipeline_final/03_events/03_03_new_foci_events_clean.sql` | `strange-math-456415-c3.mimic_analysis.new_foci_events_clean` | Eventos microbiologicos compatibles con nuevo foco |
| `analysis/pipeline_final/03_events/03_04_new_foci_flag_clean.sql` | `strange-math-456415-c3.mimic_analysis.new_foci_flag_clean` | Flag diario sin nuevo foco |
| `analysis/pipeline_final/04_daily_features/04_01_daily_features_clean.sql` | `strange-math-456415-c3.mimic_analysis.daily_features_clean` | Variables clinicas diarias |
| `analysis/pipeline_final/05_outcome/05_01_clinical_domains_sci_clean.sql` | `strange-math-456415-c3.mimic_analysis.clinical_domains_sci_clean` | Dominios clinicos de mejoria |
| `analysis/pipeline_final/05_outcome/05_02_improvement_flags_clean.sql` | `strange-math-456415-c3.mimic_analysis.improvement_flags_clean` | Mejoria diaria y sostenida |
| `analysis/pipeline_final/06_final_table/06_01_longitudinal_cohort_model_ready.sql` | `strange-math-456415-c3.mimic_analysis.longitudinal_cohort_model_ready` | Tabla longitudinal final para modelado |
| `analysis/sql/conteo_estancias_uci_monoinfeccion.sql` | Ninguna | Consulta exploratoria de conteo |
| `analysis/sql/estancias_uci.sql` | Ninguna | Consulta exploratoria de estancias UCI con microbiologia |
| `analysis/sql/estancias_uci_48h_tratamiento.sql` | Ninguna | Consulta exploratoria con tratamiento +/- 48 h |
| `analysis/sql/estancias_uci_bloques_monomicrobianos.sql` | Ninguna | Consulta exploratoria de bloques etiologico-terapeuticos |
| `analysis/sql/estancias_uci_cultivos.sql` | Ninguna | Consulta exploratoria de cultivos |
| `analysis/sql/estancias_uci_detalle.sql` | Ninguna | Consulta exploratoria de detalle |
| `analysis/sql/estancias_uci_metrica_clinica.sql` | Ninguna | Consulta exploratoria con metrica clinica |
| `analysis/sql/estancias_uci_microevents.sql` | Ninguna | Consulta exploratoria de microeventos |
| `analysis/sql/estancias_uci_monoinfeccion.sql` | Ninguna | Consulta exploratoria de monoinfeccion |
| `analysis/sql/estancias_uci_monoinfeccion_con_tratamiento_previo.sql` | Ninguna | Consulta exploratoria con tratamiento previo |
| `analysis/sql/estancias_uci_monoinfeccion_filtrada.sql` | Ninguna | Consulta exploratoria filtrada |
| `analysis/sql/estancias_uci_monoinfeccion_filtrada_comorb.sql` | Ninguna | Consulta exploratoria filtrada con comorbilidad |
| `analysis/sql/recuento_estancias_uci.sql` | Ninguna | Consulta exploratoria de recuento |
| `analysis/windows/itemid_caracteristicas_mejoria.sql` | Ninguna | Exploracion de itemids en `d_items` |
| `analysis/windows/ventana.sql` | Ninguna | Prototipo monolitico de ventanas y variables |

## 2. Fuentes MIMIC-IV trazables

| Fuente | Uso en pipeline final |
|---|---|
| `physionet-data.mimiciv_3_1_icu.icustays` | Inclusion de eventos microbiologicos dentro de estancia UCI; contexto temporal UCI |
| `physionet-data.mimiciv_3_1_hosp.microbiologyevents` | Episodio indice, antibiograma, nuevos focos microbiologicos |
| `physionet-data.mimiciv_3_1_hosp.prescriptions` | Identificacion de antibioticos, t0 y regimen basal |
| `physionet-data.mimiciv_3_1_hosp.admissions` | `deathtime` para censura de seguimiento |
| `physionet-data.mimiciv_3_1_icu.chartevents` | Signos vitales diarios |
| `physionet-data.mimiciv_3_1_hosp.labevents` | Laboratorio diario |
| `physionet-data.mimiciv_note.radiology` | Eventos de empeoramiento radiologico |

Fuentes usadas solo en scripts exploratorios o auxiliares: `physionet-data.mimiciv_3_1_hosp.diagnoses_icd`, `physionet-data.mimiciv_3_1_hosp.patients`, `physionet-data.mimiciv_3_1_icu.d_items`.

## 3. Pipeline real reconstruido

Flujo principal observado:

```text
MIMIC-IV icustays + microbiologyevents
  -> bloque_0_episode_candidates_clean
  -> bloque_0b_index_stay_clean

abx_spectrum_map_clean + prescriptions + bloque_0b_index_stay_clean
  -> bloque_t0_true
  -> baseline_regimen_detail_clean
  -> baseline_regimen_summary_clean
  -> baseline_regimen_multihot_clean

bloque_t0_true + baseline_regimen_summary_clean + baseline_regimen_multihot_clean + admissions
  -> bloque_1_base_windows_clean

bloque_1_base_windows_clean + chartevents + labevents
  -> daily_features_clean

bloque_1_base_windows_clean + radiology
  -> radiology_worsening_events_clean
  -> radiology_flag_clean

bloque_1_base_windows_clean + microbiologyevents
  -> new_foci_events_clean
  -> new_foci_flag_clean

daily_features_clean + radiology_flag_clean + new_foci_flag_clean
  -> clinical_domains_sci_clean
  -> improvement_flags_clean

bloque_1_base_windows_clean + daily_features_clean + clinical_domains_sci/improvement_flags
  -> longitudinal_cohort_model_ready
```

Dependencias especificas entre tablas intermedias:

| Tabla destino | Dependencias internas principales |
|---|---|
| `bloque_0_antibiogram_detail_clean` | `bloque_0_episode_candidates_clean` |
| `bloque_0b_index_stay_clean` | `bloque_0_episode_candidates_clean` |
| `bloque_t0_true` | `bloque_0b_index_stay_clean`, `abx_spectrum_map_clean` |
| `baseline_regimen_detail_clean` | `bloque_t0_true`, `abx_spectrum_map_clean` |
| `baseline_regimen_summary_clean` | `baseline_regimen_detail_clean` |
| `baseline_regimen_multihot_clean` | `baseline_regimen_detail_clean` |
| `bloque_1_base_windows_clean` | `bloque_t0_true`, `baseline_regimen_summary_clean`, `baseline_regimen_multihot_clean` |
| `radiology_worsening_events_clean` | `bloque_1_base_windows_clean` |
| `radiology_flag_clean` | `bloque_1_base_windows_clean`, `radiology_worsening_events_clean` |
| `new_foci_events_clean` | `bloque_1_base_windows_clean` |
| `new_foci_flag_clean` | `bloque_1_base_windows_clean`, `new_foci_events_clean` |
| `daily_features_clean` | `bloque_1_base_windows_clean` |
| `clinical_domains_sci_clean` | `daily_features_clean`, `new_foci_flag_clean`, `radiology_flag_clean` |
| `improvement_flags_clean` | `clinical_domains_sci_clean` |
| `longitudinal_cohort_model_ready` | `bloque_1_base_windows_clean`, `daily_features_clean`, `clinical_domains_sci`, `improvement_flags` |

## 4. Tablas intermedias existentes

Tablas intermedias materializadas en SQL:

- `bloque_0_episode_candidates_clean`
- `bloque_0_antibiogram_detail_clean`
- `bloque_0b_index_stay_clean`
- `abx_spectrum_map_clean`
- `bloque_t0_true`
- `baseline_regimen_detail_clean`
- `baseline_regimen_summary_clean`
- `baseline_regimen_multihot_clean`
- `bloque_1_base_windows_clean`
- `radiology_worsening_events_clean`
- `radiology_flag_clean`
- `new_foci_events_clean`
- `new_foci_flag_clean`
- `daily_features_clean`
- `clinical_domains_sci_clean`
- `improvement_flags_clean`
- `longitudinal_cohort_model_ready`

Tablas/consultas auxiliares no consumidas por el pipeline final:

- `bloque_0_antibiogram_detail_clean`: se crea, pero no se observa uso posterior en los scripts finales.
- Scripts de `analysis/sql/`: no materializan tablas y parecen consultas exploratorias previas.
- Scripts de `analysis/windows/`: prototipos o apoyo para itemids/ventanas, sin integracion directa en `pipeline_final`.

## 5. Huecos y scripts faltantes

PENDIENTE:

- `clinical_domains_sci` y `improvement_flags` sin sufijo `_clean`: el script final `06_01_longitudinal_cohort_model_ready.sql` referencia estas tablas, pero los scripts existentes crean `clinical_domains_sci_clean` e `improvement_flags_clean`. Esto probablemente rompe la ejecucion final o usa versiones antiguas no documentadas.
- `01_02_*.sql`: la numeracion salta de `01_01` a `01_03`. Puede ser intencional, pero no hay documento que explique el hueco.
- Funciones en `queries_bigquery.py` referencian nombres ausentes: `b0_cohorte.sql`, `b1_base_windows.sql`, `b2_dailyfeatures.sql`, `b2_subnew_foci_flag.sql`, `b3_clinicaldomains.sql`, `b4_improvementsflags.sql`, `pipeline_final/longitudinal_cohort.sql`.
- No se observa un manifiesto de ejecucion reproducible: falta un `README` o `run_order.md` que declare orden, proyecto/dataset BigQuery, version de MIMIC-IV, y tablas esperadas.
- No hay scripts de control de calidad: conteos por etapa, unicidad por `stay_id`, cobertura de variables diarias, tasa de missingness, ni validacion de joins.

Reconstructed:

- Los scripts en `analysis/sql` y `analysis/windows` parecen ser versiones exploratorias previas al pipeline limpio. Esta clasificacion se basa en que no contienen `CREATE OR REPLACE TABLE` y duplican logica de microbiologia, monomicrobialidad, tratamiento y ventanas.

## 6. Versiones `_clean` vs no clean

Patron observado:

- `analysis/pipeline_final` materializa casi todas las tablas con sufijo `_clean`.
- `bloque_t0_true` no tiene sufijo `_clean`, aunque esta dentro del pipeline final.
- `longitudinal_cohort_model_ready` tampoco tiene sufijo `_clean`, razonable por ser tabla final.
- `clinical_domains_sci_clean` e `improvement_flags_clean` existen, pero el script final usa `clinical_domains_sci` e `improvement_flags` sin `_clean`.

Riesgo:

- Puede haber mezcla inadvertida de tablas antiguas y nuevas en BigQuery si existen versiones no clean en el dataset `mimic_analysis`.
- Si no existen las versiones no clean, el paso final falla.

Recomendacion:

- Cambiar el script final para depender de `clinical_domains_sci_clean` e `improvement_flags_clean`, o documentar explicitamente que las versiones sin sufijo son alias materializados. Para publicacion, preferible una unica convencion.

## 7. Duplicidades de logica

Duplicidades claras:

- Cohorte UCI + microbiologia se repite en `analysis/sql/*.sql`, `analysis/windows/ventana.sql` y `pipeline_final/00_cohort/00_01_episode_candidates_clean.sql`.
- Monomicrobialidad se implementa en varias consultas historicas y en el pipeline final.
- Clasificacion por antibiotico/espectro aparece como `CASE` exploratorio en scripts antiguos y como tabla reproducible `abx_spectrum_map_clean` en el pipeline final.
- Ventanas longitudinales se prototipan en `analysis/windows/ventana.sql` y se materializan en `02_01_base_windows_clean.sql`.
- Variables clinicas diarias se prototipan en `analysis/windows/ventana.sql` y se materializan en `04_01_daily_features_clean.sql`.

Propuesta:

- Mantener `analysis/pipeline_final` como fuente canonica.
- Mover `analysis/sql` y `analysis/windows` a una carpeta `analysis/archive_exploratory/` solo si se decide reorganizar mas adelante; no borrar por trazabilidad.
- Documentar que los scripts historicos no forman parte del pipeline reproducible.

## 8. Posibles incoherencias metodologicas

Riesgos principales:

- El episodio indice se define por el primer evento microbiologico por `stay_id`; esto excluye episodios posteriores dentro de la misma estancia. Es metodologicamente defendible, pero debe declararse.
- La monomicrobialidad se define por `stay_id`, `micro_charttime` y `specimen_type`, no por `microevent_id` aislado. Debe explicarse porque afecta inclusion/exclusion.
- `bloque_0_antibiogram_detail_clean` no alimenta la seleccion de cohorte ni el outcome. Si la susceptibilidad es parte de la pregunta cientifica, falta integrarla.
- `bloque_t0_true` define `true_t0` como primer antibiotico mapeado iniciado entre 48 h antes y 48 h despues del cultivo indice. Esto mezcla tratamiento previo y posterior; debe justificarse como ventana alrededor del evento indice.
- `baseline_regimen_detail_clean` identifica antibioticos activos en `true_t0`, pero no replica los filtros de exclusion de `oral`, `enema`, `flush`, `dwell` usados en `bloque_t0_true`. Puede introducir diferencias entre deteccion de t0 y regimen basal.
- Los joins de resumen/multihot a ventanas usan `USING (stay_id)`. Es consistente con un indice por estancia, pero seria mas robusto unir tambien por `hadm_id` o `microevent_id` si se permite mas de un episodio futuro.
- `clinical_domains_sci_clean` usa referencias sin proyecto: `mimic_analysis.daily_features_clean`, `mimic_analysis.new_foci_flag_clean`, `mimic_analysis.radiology_flag_clean`. Otros scripts usan proyecto completo `strange-math-456415-c3`. Esto puede depender del proyecto por defecto de BigQuery.
- `improvement_flags_clean` tambien usa `mimic_analysis.clinical_domains_sci_clean` sin proyecto.
- El empeoramiento radiologico usa busqueda regex simple y exclusiones por terminos como `stable` o `improved`; hay riesgo de falsos positivos/negativos por contexto, negacion o idioma.
- `new_foci_events_clean` marca cualquier muestra posterior con sangre, especie distinta o tipo de muestra distinto como nuevo foco. Debe validarse clinicamente porque puede capturar vigilancia, colonizacion o cultivos no infecciosos.
- Las variables diarias usan itemids hardcoded. Existe un script auxiliar de itemids, pero no hay documento que justifique cada itemid ni su version.
- Se usa `APPROX_QUANTILES` para medianas. Es aceptable en BigQuery, pero debe indicarse que son medianas aproximadas.
- No hay control explicito de pacientes con multiples `hadm_id` o multiples estancias salvo seleccion por `stay_id`.
- No se observan exclusiones por edad, comorbilidad o diagnosticos en el pipeline final, aunque si aparecen en scripts exploratorios. Si forman parte del protocolo, faltan en `pipeline_final`.

## 9. Pasos no documentados

PENDIENTE de documentacion:

- Criterios de inclusion de microorganismos y especimenes excluidos.
- Justificacion de la ventana `[-48h, +48h]` para antibioticos alrededor del cultivo.
- Definicion formal de t0: cultivo vs primer antibiotico mapeado.
- Criterios de censura: fin de UCI, muerte, 30 dias.
- Definicion de mejoria clinica y umbral `n_domains_ok >= 3`.
- Manejo de missingness en dominios clinicos.
- Acceso y version de `mimiciv_note.radiology`.
- Orden de ejecucion y dataset destino.
- Checks esperados por etapa: numero de estancias, numero de dias, duplicados, missingness.

## 10. Propuesta de reorganizacion

Estructura recomendada:

```text
analysis/
  README.md
  PIPELINE_AUDIT.md
  pipeline_final/
    00_cohort/
    01_antibiotics_t0/
    02_windows/
    03_events/
    04_daily_features/
    05_outcome/
    06_final_table/
    99_qc/
  archive_exploratory/
    sql/
    windows/
```

Convenciones recomendadas:

- Un unico dataset destino parametrizable: `PROJECT_ID.DATASET`.
- Un unico estilo de nombres: o todas las tablas intermedias con `_clean`, o ninguna; evitar mezclar.
- Documentar cada script con cabecera minima: objetivo, tabla creada, dependencias, unidad de analisis, claves esperadas, criterios clinicos.
- Crear `analysis/pipeline_final/RUN_ORDER.md` con el orden exacto.
- Crear `analysis/pipeline_final/99_qc/` con consultas de validacion antes de producir notebooks.

## 11. Orden recomendado de reconstruccion

Orden canonico para reconstruir el pipeline:

1. `00_01_episode_candidates_clean.sql`
2. `00_02_antibiogram_detail_clean.sql`
3. `00_03_index_stay_clean.sql`
4. `01_01_abx_spectrum_map_clean.sql`
5. `01_01_t0_true.sql`
6. `01_03_baseline_regimen_detail_clean.sql`
7. `01_04_baseline_regimen_summary_clean.sql`
8. `01_05_baseline_regimen_multihot_clean.sql`
9. `02_01_base_windows_clean.sql`
10. `03_01_radiology_worsening_events_clean.sql`
11. `03_02_radiology_flag_clean.sql`
12. `03_03_new_foci_events_clean.sql`
13. `03_04_new_foci_flag_clean.sql`
14. `04_01_daily_features_clean.sql`
15. `05_01_clinical_domains_sci_clean.sql`
16. `05_02_improvement_flags_clean.sql`
17. `06_01_longitudinal_cohort_model_ready.sql`

Antes del paso 17 debe resolverse la incoherencia `_clean` vs no clean en `clinical_domains_sci` e `improvement_flags`.

## 12. Prioridades inmediatas antes de notebooks

1. Corregir o documentar las referencias no clean del script final.
2. Crear un manifiesto de ejecucion reproducible.
3. Agregar consultas QC por etapa.
4. Decidir si `bloque_0_antibiogram_detail_clean` sera usado para exposicion, estratificacion o descripcion.
5. Sincronizar `queries_bigquery.py` con los nombres reales de `pipeline_final`, o marcarlo como legacy.
6. Documentar los criterios clinicos y temporales antes de construir notebooks analiticos.
