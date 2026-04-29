# Auditoria metodologica del pipeline amr_days / MIMIC-IV

Fecha de auditoria: 2026-04-30  
Alcance: scripts SQL/Python del repositorio local, con foco en `analysis/pipeline_final/`. No se ejecutaron consultas contra BigQuery. Las definiciones se derivan del codigo disponible.

## Lectura ejecutiva

El pipeline canonico actual construye una cohorte longitudinal diaria de estancias UCI con cultivo positivo para microorganismos seleccionados, monomicrobiano en el evento indice, y con antibiotico mapeado iniciado dentro de una ventana de 48 horas antes/despues del cultivo indice. El seguimiento diario empieza en `true_t0`, definido por codigo como el primer inicio de antibiotico mapeado dentro de esa ventana, no como el momento del cultivo.

La tabla final pretendida es `longitudinal_cohort_model_ready`, a nivel `stay_id`-dia. Integra microbiologia indice, regimen antibiotico basal, ventanas diarias, variables fisiologicas/laboratorio, eventos radiologicos/microbiologicos posteriores y flags de mejoria clinica.

Error grave detectado, sin corregir: `06_01_longitudinal_cohort_model_ready.sql` referencia `clinical_domains_sci` e `improvement_flags`, pero los scripts canonicos crean `clinical_domains_sci_clean` e `improvement_flags_clean`. Si las tablas sin sufijo no existen, el paso final falla; si existen, puede mezclar versiones antiguas.

## Pipeline global

### Orden correcto de ejecucion

1. `analysis/pipeline_final/00_cohort/00_01_episode_candidates_clean.sql`
2. `analysis/pipeline_final/00_cohort/00_02_antibiogram_detail_clean.sql`
3. `analysis/pipeline_final/00_cohort/00_03_index_stay_clean.sql`
4. `analysis/pipeline_final/01_antibiotics_t0/01_01_abx_spectrum_map_clean.sql`
5. `analysis/pipeline_final/01_antibiotics_t0/01_01_t0_true.sql`
6. `analysis/pipeline_final/01_antibiotics_t0/01_03_baseline_regimen_detail_clean.sql`
7. `analysis/pipeline_final/01_antibiotics_t0/01_04_baseline_regimen_summary_clean.sql`
8. `analysis/pipeline_final/01_antibiotics_t0/01_05_baseline_regimen_multihot_clean.sql`
9. `analysis/pipeline_final/02_windows/02_01_base_windows_clean.sql`
10. `analysis/pipeline_final/03_events/03_01_radiology_worsening_events_clean.sql`
11. `analysis/pipeline_final/03_events/03_02_radiology_flag_clean.sql`
12. `analysis/pipeline_final/03_events/03_03_new_foci_events_clean.sql`
13. `analysis/pipeline_final/03_events/03_04_new_foci_flag_clean.sql`
14. `analysis/pipeline_final/04_daily_features/04_01_daily_features_clean.sql`
15. `analysis/pipeline_final/05_outcome/05_01_clinical_domains_sci_clean.sql`
16. `analysis/pipeline_final/05_outcome/05_02_improvement_flags_clean.sql`
17. `analysis/pipeline_final/06_final_table/06_01_longitudinal_cohort_model_ready.sql`

Antes del paso 17 hay que resolver la incoherencia `_clean` frente a no `_clean`.

### Diagrama textual del flujo

```text
icustays + microbiologyevents
  -> bloque_0_episode_candidates_clean
  -> bloque_0b_index_stay_clean

bloque_0_episode_candidates_clean + microbiologyevents
  -> bloque_0_antibiogram_detail_clean

abx_spectrum_map_clean + prescriptions + bloque_0b_index_stay_clean
  -> bloque_t0_true
  -> baseline_regimen_detail_clean
  -> baseline_regimen_summary_clean
  -> baseline_regimen_multihot_clean

bloque_t0_true + baseline_regimen_* + admissions
  -> bloque_1_base_windows_clean

bloque_1_base_windows_clean + chartevents + labevents
  -> daily_features_clean

bloque_1_base_windows_clean + radiology notes
  -> radiology_worsening_events_clean
  -> radiology_flag_clean

bloque_1_base_windows_clean + microbiologyevents
  -> new_foci_events_clean
  -> new_foci_flag_clean

daily_features_clean + new_foci_flag_clean + radiology_flag_clean
  -> clinical_domains_sci_clean
  -> improvement_flags_clean

bloque_1_base_windows_clean + daily_features_clean + outcome flags
  -> longitudinal_cohort_model_ready
```

## Entregable 1 - Resumen ejecutivo del proyecto

El proyecto aborda si pacientes criticos con infecciones confirmadas por cultivos positivos para microorganismos clinicamente relevantes pueden mostrar mejoria clinica sostenida antes de completar duraciones antibioticas estandar.

La poblacion que el codigo estudia son estancias UCI de MIMIC-IV con cultivo positivo durante la estancia UCI para una lista cerrada de microorganismos: `escherichia coli`, `klebsiella pneumoniae`, `klebsiella aerogenes`, `enterobacter cloacae`, `enterobacter aerogenes`, `pseudomonas aeruginosa`, `acinetobacter baumannii`, `stenotrophomonas maltophilia`, `enterococcus faecium` y `staphylococcus aureus`.

La exposicion disponible es el regimen antibiotico basal activo en `true_t0`, resumido por numero de antibioticos, nivel maximo de espectro y variables multihot de farmacos. El codigo todavia no calcula duracion total de tratamiento, desescalada, interrupcion ni exposicion antibiotica dinamica dia a dia.

El outcome principal construido es `sustained_improvement`: mejoria diaria en dos dias consecutivos. `improved_today` exige ausencia de nuevo foco, estabilidad radiologica y al menos 3 de 5 dominios fisiologicos/laboratorio favorables.

El enfoque longitudinal aporta una fila por estancia-dia desde T0 hasta alta UCI, muerte o 30 dias, lo que permite estudiar trayectorias clinicas y tiempo hasta mejoria, pero exige manejar censura, muerte, alta de UCI y sesgos por supervivencia.

## Entregable 2 - Tabla de scripts y tablas

| Orden | Script | Tabla generada | Tablas de entrada | Nivel de agregacion | Objetivo | Variables clave | Riesgos/QC |
|---:|---|---|---|---|---|---|---|
| 1 | `00_01_episode_candidates_clean.sql` | `bloque_0_episode_candidates_clean` | `icustays`, `microbiologyevents` | evento microbiologico en estancia | Identificar cultivos positivos dentro de UCI para organismos y muestras elegibles. | `microevent_id`, `index_charttime`, `organism_name`, `specimen_type`, `is_monomicrobial_event` | Verificar exactitud de organismos, muestras excluidas, duplicados por evento y que `index_charttime` cae dentro de UCI. |
| 2 | `00_02_antibiogram_detail_clean.sql` | `bloque_0_antibiogram_detail_clean` | `bloque_0_episode_candidates_clean`, `microbiologyevents` | antibiograma por evento-antibiotico | Extraer interpretaciones R/S/I del antibiograma del evento indice. | `susceptibility_ab_name`, `susceptibility_interpretation` | No se usa despues; decidir si se integra para MDR/sensibilidad. |
| 3 | `00_03_index_stay_clean.sql` | `bloque_0b_index_stay_clean` | `bloque_0_episode_candidates_clean` | una fila por `stay_id` | Elegir el primer evento microbiologico elegible por estancia UCI. | `rn=1`, `index_charttime`, `microevent_id` | Excluye episodios posteriores; QC de una fila por `stay_id`. |
| 4 | `01_01_abx_spectrum_map_clean.sql` | `abx_spectrum_map_clean` | mapa manual | antibiotico/patron | Mapear nombres de farmacos a antibiotico estandar, espectro y dominio de cobertura. | `pattern`, `abx_name_std`, `spectrum_level`, `coverage_domain` | Regex puede solapar; validar prioridades, antibioticos omitidos y vias. |
| 5 | `01_01_t0_true.sql` | `bloque_t0_true` | `bloque_0b_index_stay_clean`, `prescriptions`, `abx_spectrum_map_clean` | una fila por `stay_id` con T0 | Definir T0 como primer inicio de antibiotico mapeado en ventana cultivo +/- 48 h. | `true_t0`, `drug_raw`, ventana +/-48 h | Solo incluye estancias con antibiotico mapeado; T0 puede ser previo al cultivo. |
| 6 | `01_03_baseline_regimen_detail_clean.sql` | `baseline_regimen_detail_clean` | `bloque_t0_true`, `prescriptions`, `abx_spectrum_map_clean` | antibiotico activo en T0 | Listar antibioticos activos en T0. | `abx_name_std`, `spectrum_level`, `start_ts`, `stop_ts` | No replica filtros `oral/enema/flush/dwell`; puede incluir tratamientos no sistemicos. |
| 7 | `01_04_baseline_regimen_summary_clean.sql` | `baseline_regimen_summary_clean` | `baseline_regimen_detail_clean` | una fila por `stay_id` | Resumir regimen basal. | `n_abx_t0`, `spectrum_level_t0`, `has_broad_t0`, `has_gp_resistant_t0`, `has_gn_mdr_t0` | Validar nulos y consistencia con detalle. |
| 8 | `01_05_baseline_regimen_multihot_clean.sql` | `baseline_regimen_multihot_clean` | `baseline_regimen_detail_clean` | una fila por `stay_id` | Crear indicadores de antibioticos especificos en T0. | `t0_vancomycin`, `t0_pip_tazo`, `t0_meropenem`, etc. | Falta multihot para algunos antibioticos del mapa, por ejemplo colistin/amikacin/tigecycline. |
| 9 | `02_01_base_windows_clean.sql` | `bloque_1_base_windows_clean` | `bloque_t0_true`, `baseline_regimen_*`, `admissions` | `stay_id`-dia | Expandir seguimiento diario desde T0 hasta alta UCI, muerte o 30 dias. | `day_idx`, `window_start`, `window_end`, `followup_end` | Pacientes en UCI >30 dias censurados a 30; alta y muerte compiten con outcome. |
| 10 | `03_01_radiology_worsening_events_clean.sql` | `radiology_worsening_events_clean` | `bloque_1_base_windows_clean`, `mimiciv_note.radiology` | evento radiologico | Detectar empeoramiento radiologico por expresiones regulares despues de T0. | `radiology_worsening_flag`, `charttime` | Regex simple; alto riesgo de negacion/contexto. |
| 11 | `03_02_radiology_flag_clean.sql` | `radiology_flag_clean` | `bloque_1_base_windows_clean`, `radiology_worsening_events_clean` | `stay_id`-dia | Marcar dias sin empeoramiento radiologico hasta el primer evento. | `radiology_stable_flag` | Sin nota radiologica equivale a estable; requiere validacion. |
| 12 | `03_03_new_foci_events_clean.sql` | `new_foci_events_clean` | `bloque_1_base_windows_clean`, `microbiologyevents` | evento microbiologico posterior | Detectar posible nuevo foco: sangre, muestra distinta u organismo distinto despues de T0. | `new_focus_flag` | Puede capturar colonizacion/vigilancia; no filtra tipos de muestra como en indice. |
| 13 | `03_04_new_foci_flag_clean.sql` | `new_foci_flag_clean` | `bloque_1_base_windows_clean`, `new_foci_events_clean` | `stay_id`-dia | Marcar dias sin nuevo foco hasta el primer evento. | `no_new_foci_flag` | Asume ausencia de nuevos cultivos positivos como ausencia de nuevo foco. |
| 14 | `04_01_daily_features_clean.sql` | `daily_features_clean` | `bloque_1_base_windows_clean`, `chartevents`, `labevents` | `stay_id`-dia | Agregar signos vitales y laboratorio por mediana diaria. | `HR_median`, `MAP_median`, `Temp_median`, `WBC_median`, `Lactate_median`, `spo2fio2_ratio` | Itemids hardcoded, medianas aproximadas, missingness importante. |
| 15 | `05_01_clinical_domains_sci_clean.sql` | `clinical_domains_sci_clean` | `daily_features_clean`, `new_foci_flag_clean`, `radiology_flag_clean` | `stay_id`-dia | Convertir variables diarias en dominios clinicos binarios. | `temp_in_range`, `wbc_normalizing`, `hemo_stable`, `lactate_normalizing`, `resp_improving` | Umbrales requieren confirmacion clinica; referencias sin proyecto completo. |
| 16 | `05_02_improvement_flags_clean.sql` | `improvement_flags_clean` | `clinical_domains_sci_clean` | `stay_id`-dia | Definir mejoria diaria y sostenida. | `n_domains_ok`, `improved_today`, `sustained_improvement` | Missing se cuenta como 0 en dominios fisiologicos, pero foci/radiologia nulos se tratan como OK. |
| 17 | `06_01_longitudinal_cohort_model_ready.sql` | `longitudinal_cohort_model_ready` | `bloque_1_base_windows_clean`, `daily_features_clean`, `clinical_domains_sci`, `improvement_flags` | `stay_id`-dia | Integrar la tabla final lista para modelado. | todas las covariables y outcome | Error de dependencias sin `_clean`; QC de unicidad `stay_id, day_idx`. |

Scripts historicos en `analysis/sql/*.sql` y `analysis/windows/*.sql`: no materializan tablas del pipeline final. Documentan prototipos de comorbilidad, tratamiento previo/posterior, bloques microorganismo-antibiotico y ventanas, pero no deben presentarse como definicion actual salvo confirmacion.

## Detalle por tabla intermedia

### `bloque_0_episode_candidates_clean`

Objetivo: define candidatos de episodio infeccioso por cultivo positivo durante UCI. Une `icustays` con `microbiologyevents` por `hadm_id` y exige que `micro.charttime` este entre `icu.intime` y `icu.outtime`.

Filtros reales: `charttime`, `hadm_id` y `org_name` no nulos; organismo en lista cerrada; tipo de muestra no incluido en la lista de muestras excluidas. La monomicrobialidad se calcula agrupando por `stay_id`, `micro_charttime` y `specimen_type`, con `COUNT(DISTINCT organism_name)=1`.

Decision implicita: el evento monomicrobiano no se define por `microevent_id` aislado, sino por coincidencia temporal y tipo de muestra. En presentacion clinica: "seleccionamos cultivos positivos durante UCI para patogenos predefinidos y excluimos muestras probablemente no representativas; nos quedamos con eventos monomicrobianos".

QC recomendado: conteo por organismo/muestra, duplicados por `microevent_id`, eventos fuera de UCI, porcentaje excluido por polimicrobialidad.

### `bloque_0_antibiogram_detail_clean`

Objetivo: extraer antibiogramas R/S/I asociados al evento indice candidato. No alimenta el pipeline posterior.

Decision implicita: se preserva susceptibilidad, pero no se usa para definir MDR, exposicion adecuada ni grupos microorganismo-antibiotico. En presentacion: "tenemos antibiograma disponible como tabla auxiliar, pendiente de integrar".

QC recomendado: proporciones R/S/I por organismo-antibiotico, eventos indice sin antibiograma, antibioticos repetidos por evento.

### `bloque_0b_index_stay_clean`

Objetivo: seleccionar un unico episodio indice por estancia UCI mediante `ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY index_charttime, microevent_id)`.

Decision implicita: se analiza el primer episodio elegible por estancia. Episodios posteriores quedan excluidos del analisis principal.

QC recomendado: una fila por `stay_id`, distribucion de tiempo desde ingreso UCI hasta cultivo, sensibilidad considerando primer episodio por `hadm_id` o por paciente.

### `abx_spectrum_map_clean`

Objetivo: tabla manual de regex para mapear `prescriptions.drug` a nombres estandarizados y niveles de espectro: 1 estrecho, 2 intermedio, 3 amplio, 4 muy amplio.

Decision implicita: el nivel de espectro es ordinal y clinicamente simplificado; no depende de microorganismo ni antibiograma.

QC recomendado: farmacos no mapeados frecuentes, solapamientos de regex, revision de dominios `gp_resistente`, `gn_multirresistente`, `general_reserva`.

### `bloque_t0_true`

Objetivo: definir T0 terapeutico como `MIN(start_ts)` entre antibioticos mapeados iniciados de 48 h antes a 48 h despues del cultivo indice. Excluye nombres con `oral`, `enema`, `flush`, `dwell` en esta etapa.

Inclusion implicita: solo estancias con al menos un antibiotico mapeado en esa ventana pasan a T0. T0 puede preceder al cultivo.

QC recomendado: distribucion `true_t0 - index_charttime`, estancias perdidas por no tener antibiotico, lista de farmacos excluidos por via/texto.

### `baseline_regimen_detail_clean`, `summary` y `multihot`

Objetivo: identificar antibioticos activos exactamente en `true_t0`, resumir combinacion basal y crear indicadores por farmaco.

Regimen activo: `start_ts <= true_t0` y `stop_ts IS NULL OR stop_ts > true_t0`.

Riesgo principal: el detalle basal no aplica los filtros de `oral/enema/flush/dwell` que si se aplican para localizar T0. Puede reintroducir medicamentos no deseados si coinciden con el mapa.

QC recomendado: concordancia entre `n_abx_t0` y detalle; farmacos activos en detalle con palabras de via no sistemica; cobertura de multihot frente a todos los `abx_name_std`.

### `bloque_1_base_windows_clean`

Objetivo: generar una tabla diaria desde T0. `followup_end` es el minimo de `icu_outtime`, `deathtime` si existe, y `t0 + 30 dias`.

Ventanas: `day_idx=0..floor(hours_followup/24)`, con `window_start=t0+day_idx*24h` y `window_end=min(t0+(day_idx+1)*24h, followup_end)`.

Decision implicita: seguimiento intra-UCI, censurado por alta UCI, muerte o 30 dias. Si `t0 >= followup_end`, la estancia se excluye.

QC recomendado: numero de dias por estancia, ventanas de duracion cero, muertes antes de T0, seguimiento truncado a 30 dias.

### Eventos radiologicos y nuevos focos

Radiologia: busca textos posteriores a T0 con patrones como `new consolidation`, `new infiltrate`, `worsening consolidation`, y excluye notas con `improved`, `stable`, `unchanged`, etc. Se transforma en `radiology_stable_flag`, que vale 1 hasta el primer empeoramiento y 0 desde ese dia.

Nuevo foco: cualquier microbiologia posterior con muestra de sangre, tipo de muestra distinto al basal u organismo distinto al basal. Se transforma en `no_new_foci_flag`.

Riesgos: ambos son proxies. La radiologia puede fallar por negacion/contexto; nuevo foco puede mezclar infeccion, colonizacion y vigilancia.

### `daily_features_clean`

Objetivo: calcular medianas diarias aproximadas de signos vitales y laboratorio. Usa rangos plausibles para limpiar valores extremos y convierte temperatura/FIO2/SpO2 cuando procede.

Variables: HR, MAP, SysBP, DiasBP, Temp, RR, SpO2, FiO2, WBC, Lactate, Creatinine, Bilirubin, Platelets, Hgb, `spo2fio2_ratio`.

QC recomendado: missingness por variable/dia, validez de itemids, unidades de FiO2, comparacion `SpO2/FiO2` con valores esperados.

### Outcome: `clinical_domains_sci_clean` e `improvement_flags_clean`

Dominios: temperatura 36-38, WBC 4-12, MAP >=65, lactato <2, SpO2/FiO2 >=240, estabilidad radiologica y ausencia de nuevos focos.

`n_domains_ok`: suma de los 5 dominios fisiologicos/laboratorio, con nulos como 0.  
`improved_today`: 1 si no hay nuevo foco, radiologia estable y `n_domains_ok >= 3`.  
`sustained_improvement`: 1 si `improved_today=1` y el dia anterior tambien fue 1.

Limitacion: no mide curacion microbiologica directa, ni resolucion de vasopresores/ventilacion, ni supervivencia; los thresholds son proxies que requieren validacion clinica.

## Entregable 3 - Diccionario de variables

| Variable | Tabla donde se crea | Tipo | Definicion operativa | Referencia temporal | Significado clinico | Uso en analisis |
|---|---|---|---|---|---|---|
| `subject_id` | origen MIMIC | entero | Identificador de paciente | global | Paciente | clustering/confusion |
| `hadm_id` | origen MIMIC | entero | Ingreso hospitalario | global | Episodio hospitalario | joins hospitalarios |
| `stay_id` | `icustays` | entero | Estancia UCI | global | Unidad principal de cohorte | clave de paciente-UCI |
| `microevent_id` | `microbiologyevents` | entero | Evento microbiologico indice | cultivo | Cultivo positivo | trazabilidad |
| `index_charttime` | `bloque_0_episode_candidates_clean` | timestamp | Hora del cultivo indice | cultivo | Ancla microbiologica | definicion de episodio |
| `episode_anchor_source` | `bloque_0_episode_candidates_clean` | texto | Literal `microbiology_charttime` | cultivo | Fuente de anclaje | documentacion |
| `icu_intime`, `icu_outtime` | `icustays` | timestamp | Entrada y salida UCI | estancia UCI | Contexto UCI | inclusion/censura |
| `index_within_icu` | `bloque_0_episode_candidates_clean` | booleano | Siempre TRUE tras filtro temporal | cultivo/UCI | Cultivo dentro de UCI | QC |
| `hours_from_icu_intime_to_index` | `bloque_0_episode_candidates_clean` | numerico | Horas de ingreso UCI a cultivo | cultivo | Momento de infeccion | descripcion |
| `hours_from_index_to_icu_outtime` | `bloque_0_episode_candidates_clean` | numerico | Horas de cultivo a salida UCI | cultivo | Seguimiento potencial | QC |
| `specimen_type` | `bloque_0_episode_candidates_clean` | texto | `LOWER(spec_type_desc)` | cultivo | Tipo de muestra | estratificacion |
| `organism_name` | `bloque_0_episode_candidates_clean` | texto | `LOWER(org_name)` | cultivo | Microorganismo causal/proxy | estratificacion |
| `is_monomicrobial_event` | `bloque_0_episode_candidates_clean` | booleano | TRUE si un organismo por `stay_id`-hora-muestra | cultivo | Monoinfeccion | inclusion |
| `susceptibility_ab_name` | `bloque_0_antibiogram_detail_clean` | texto | Antibiotico del antibiograma | cultivo | Susceptibilidad | pendiente |
| `susceptibility_interpretation` | `bloque_0_antibiogram_detail_clean` | texto | R/S/I | cultivo | Resistencia/sensibilidad | pendiente |
| `true_t0` / `t0` | `bloque_t0_true` | timestamp | Primer antibiotico mapeado en cultivo +/-48 h | tratamiento | Inicio terapeutico basal | tiempo cero |
| `abx_name_std` | `baseline_regimen_detail_clean` | texto | Nombre antibiotico estandarizado por regex | T0 | Farmaco | exposicion |
| `spectrum_level` | `abx_spectrum_map_clean` | entero | 1 estrecho, 2 intermedio, 3 amplio, 4 muy amplio | T0 | Intensidad de espectro | exposicion |
| `spectrum_label` | `abx_spectrum_map_clean` | texto | Etiqueta del nivel | T0 | Interpretacion clinica | descripcion |
| `coverage_domain` | `abx_spectrum_map_clean` | texto | Dominio de cobertura | T0 | GP resistente/GN MDR/etc. | estratificacion |
| `n_abx_t0` | `baseline_regimen_summary_clean` | entero | Numero de antibioticos activos en T0 | T0 | Monoterapia/combinacion | exposicion |
| `spectrum_level_t0` | `baseline_regimen_summary_clean` | entero | Maximo nivel de espectro activo en T0 | T0 | Espectro basal | exposicion |
| `has_broad_t0` | `baseline_regimen_summary_clean` | binaria | 1 si algun `spectrum_level >=3` | T0 | Tratamiento amplio | exposicion |
| `has_gp_resistant_t0` | `baseline_regimen_summary_clean` | binaria | 1 si dominio `gp_resistente` | T0 | Cobertura GP resistente | exposicion |
| `has_gn_mdr_t0` | `baseline_regimen_summary_clean` | binaria | 1 si dominio `gn_multirresistente` | T0 | Cobertura GN MDR | exposicion |
| `t0_*` | `baseline_regimen_multihot_clean` | binaria | Indicador de farmaco activo en T0 | T0 | Antibiotico basal especifico | predictores |
| `deathtime` | `admissions` | timestamp | Fecha/hora de muerte hospitalaria | seguimiento | Evento terminal | censura/competidor |
| `followup_end` | `bloque_1_base_windows_clean` | timestamp | Minimo de alta UCI, muerte, T0+30d | seguimiento | Fin observacion | censura |
| `day_idx` | `bloque_1_base_windows_clean` | entero | Dia desde T0 | seguimiento | Tiempo longitudinal | eje temporal |
| `window_start`, `window_end` | `bloque_1_base_windows_clean` | timestamp | Ventana diaria de 24 h o truncada | seguimiento | Periodo de medicion | joins diarios |
| `HR_median` | `daily_features_clean` | numerico | Mediana diaria de frecuencia cardiaca | dia | Respuesta fisiologica | predictor/outcome |
| `MAP_median` | `daily_features_clean` | numerico | Mediana diaria de presion arterial media | dia | Estabilidad hemodinamica | outcome |
| `Temp_median` | `daily_features_clean` | numerico | Mediana diaria de temperatura C | dia | Fiebre/resolucion | outcome |
| `RR_median` | `daily_features_clean` | numerico | Mediana diaria de frecuencia respiratoria | dia | Estado respiratorio | predictor |
| `SpO2_median`, `FiO2_median` | `daily_features_clean` | numerico | Medianas diarias | dia | Oxigenacion/soporte | outcome |
| `WBC_median` | `daily_features_clean` | numerico | Mediana diaria leucocitos | dia | Inflamacion/infeccion | outcome |
| `Lactate_median` | `daily_features_clean` | numerico | Mediana diaria lactato | dia | Hipoperfusion/gravedad | outcome |
| `Creatinine_median` | `daily_features_clean` | numerico | Mediana diaria creatinina | dia | Funcion renal | confusor/seguridad |
| `Bilirubin_median` | `daily_features_clean` | numerico | Mediana diaria bilirrubina | dia | Funcion hepatica | gravedad |
| `Platelets_median` | `daily_features_clean` | numerico | Mediana diaria plaquetas | dia | Sepsis/gravedad | gravedad |
| `Hgb_median` | `daily_features_clean` | numerico | Mediana diaria hemoglobina | dia | Estado hematologico | confusor |
| `spo2fio2_ratio` | `daily_features_clean` | numerico | SpO2 / FiO2 | dia | Oxigenacion | dominio respiratorio |
| `radiology_worsening_flag` | `radiology_worsening_events_clean` | binaria | Regex positivo de empeoramiento | post-T0 | Empeoramiento radiologico | evento |
| `radiology_stable_flag` | `radiology_flag_clean` | binaria | 1 antes de primer empeoramiento, 0 desde entonces | dia | Estabilidad radiologica | outcome |
| `new_focus_flag` | `new_foci_events_clean` | binaria | Cultivo posterior sangre/muestra distinta/org distinto | post-T0 | Posible nuevo foco | evento |
| `no_new_foci_flag` | `new_foci_flag_clean` | binaria | 1 antes de primer nuevo foco, 0 desde entonces | dia | Sin nueva infeccion/foco | outcome |
| `temp_in_range` | `clinical_domains_sci_clean` | binaria/null | Temp 36-38 | dia | Afebril/normotermia | outcome |
| `wbc_normalizing` | `clinical_domains_sci_clean` | binaria/null | WBC 4-12 | dia | Respuesta inflamatoria | outcome |
| `hemo_stable` | `clinical_domains_sci_clean` | binaria/null | MAP >=65 | dia | Estabilidad hemodinamica | outcome |
| `lactate_normalizing` | `clinical_domains_sci_clean` | binaria/null | Lactato <2 | dia | Perfusion | outcome |
| `resp_improving` | `clinical_domains_sci_clean` | binaria/null | SpO2/FiO2 >=240 | dia | Mejoria respiratoria | outcome |
| `n_domains_ok` | `improvement_flags_clean` | entero | Suma de 5 dominios fisiologicos con nulos=0 | dia | Carga de mejoria | outcome intermedio |
| `improved_today` | `improvement_flags_clean` | binaria | Sin foco, radio estable, `n_domains_ok>=3` | dia | Mejoria diaria | outcome diario |
| `sustained_improvement` | `improvement_flags_clean` | binaria | `improved_today` actual y previo iguales a 1 | dia | Mejoria sostenida | outcome principal |

Variables demograficas como edad, sexo y raza: no identificadas en el pipeline final. Aparecen referencias exploratorias a `patients`, pero no estan integradas.

## Entregable 4 - Criterios de inclusion y exclusion

### Pacientes/estancias UCI

Incluidos por codigo: estancias UCI (`stay_id`) con cultivo positivo durante el intervalo `icu.intime` a `icu.outtime`; una estancia puede aportar como maximo un episodio indice.

Excluidos por codigo: estancias sin cultivo elegible dentro de UCI; estancias sin antibiotico mapeado en ventana +/-48 h del cultivo; estancias con `t0 >= followup_end`.

No identificado en pipeline final: edad adulta, primera estancia por paciente, diagnostico de sepsis, ventilacion mecanica, shock, inmunosupresion, embarazo o limitacion terapeutica.

### Microbiologicos

Incluidos: cultivos con `org_name` no nulo y dentro de la lista cerrada de 10 microorganismos.

Excluidos: eventos polimicrobianos segun agrupacion por `stay_id`, `micro_charttime` y `specimen_type`.

### Microorganismos

Incluidos por codigo: `E. coli`, `K. pneumoniae`, `K. aerogenes`, `Enterobacter cloacae`, `Enterobacter aerogenes`, `P. aeruginosa`, `A. baumannii`, `S. maltophilia`, `E. faecium`, `S. aureus`.

No identificado en pipeline final: definicion de MDR/XDR/PDR, MRSA, VRE, ESBL, CRE, resistencia a carbapenem o adecuacion segun antibiograma.

### Muestras

Excluidas por codigo en episodio indice: `swab`, `fluid,other`, `foreign body`, `foot culture`, `fluid received in blood culture bottles`, `ear`, `fluid wound`, `dialysis fluid`, `skin scrapings`, `foreign body - sonication culture`, `eye`.

No identificado: jerarquia clinica de muestras, colonizacion vs infeccion, contaminantes, repeticion de hemocultivos.

### Temporales

Cultivo indice: dentro de estancia UCI.  
T0: primer antibiotico mapeado iniciado entre 48 h antes y 48 h despues del cultivo indice.  
Seguimiento: desde T0 hasta alta UCI, muerte o 30 dias, lo que ocurra antes.  
Ventanas: intervalos diarios de 24 h desde T0, con ultima ventana truncada.

### Antibioticos

Incluidos: farmacos que hacen match con `abx_spectrum_map_clean`.  
Excluidos en definicion de T0: `drug` que contiene `oral`, `enema`, `flush`, `dwell`.  
Riesgo: esos filtros no se repiten en `baseline_regimen_detail_clean`.

## Entregable 5 - Definicion del outcome

Outcome principal en codigo: `sustained_improvement`.

Es un outcome longitudinal diario, binario, a nivel `stay_id`-dia. Se activa cuando el paciente cumple `improved_today=1` durante dos dias consecutivos, usando `LAG(improved_today)` dentro de cada `stay_id`.

Componentes de `improved_today`:

- `no_new_foci_flag=1` o nulo tratado como 1.
- `radiology_stable_flag=1` o nulo tratado como 1.
- `n_domains_ok >= 3`.

Dominios fisiologicos/laboratorio:

- Temperatura en rango: `Temp_median` entre 36 y 38.
- Leucocitos normalizando: `WBC_median` entre 4 y 12.
- Estabilidad hemodinamica: `MAP_median >= 65`.
- Lactato normalizando: `Lactate_median < 2`.
- Respiratorio mejorando: `spo2fio2_ratio >= 240`.

Mejoria sostenida: dos dias consecutivos de mejoria diaria. En la practica, el primer dia que puede ser sostenido es `day_idx >= 1`.

Limitaciones:

- No usa curacion microbiologica negativa; usa ausencia de nuevo foco como proxy.
- No modela muerte ni alta UCI como eventos competitivos, solo censura por `followup_end`.
- Missingness fisiologico cuenta como dominio no cumplido, pero missing en radiologia/foco cuenta como favorable.
- No incorpora vasopresores, ventilacion mecanica, SOFA, antibiotico adecuado ni control de foco.
- Los umbrales requieren confirmacion clinica y bibliografica.

## Entregable 6 - Variables predictoras y confundidoras

### Demograficas

Disponibles en tabla final: `subject_id` como identificador, pero no edad/sexo/raza.  
Faltan para analisis: edad, sexo, raza/etnia, peso, procedencia, hospitalizacion previa.

### Clinicas

Disponibles: `icu_intime`, `icu_outtime`, `deathtime`, `followup_end`, medianas diarias de constantes y laboratorios.  
Faltan/revisar: SOFA/SAPS/OASIS, ventilacion mecanica, vasopresores, foco clinico, cirugia/control de foco, servicio, reingreso UCI.

### Microbiologicas

Disponibles: `organism_name`, `specimen_type`, `microevent_id`, `index_charttime`, monomicrobialidad.  
Auxiliar no integrado: antibiograma R/S/I.  
Faltan/revisar: MDR, ESBL, CRE, MRSA, VRE, susceptibilidad al regimen recibido, carga por hemocultivos persistentes.

### Tratamiento antibiotico

Disponibles: `n_abx_t0`, `spectrum_level_t0`, `has_broad_t0`, `has_gp_resistant_t0`, `has_gn_mdr_t0`, multihot de farmacos basales.  
Faltan/revisar: duracion total, dias de terapia, cambios posteriores, desescalada/escalada, adecuacion al antibiograma, via/dosis, combinacion sinergica.

### Longitudinales diarias

Disponibles: signos vitales/labs por dia, dominios clinicos, flags de radiologia y nuevos focos, `day_idx`.

### Gravedad/proxies

Disponibles como proxies: MAP, lactato, SpO2/FiO2, plaquetas, creatinina, bilirrubina.  
Faltan: puntuaciones validadas y soporte organico explicito.

### Temporales

Disponibles: tiempo UCI-cultivo, cultivo-salida UCI, T0, ventanas diarias, censura a 30 dias.  
Faltan/revisar: tiempo sintomas-tratamiento, tiempo antibiograma-T0, tiempo a adecuacion, calendario absoluto/epoca.

## Entregable 7 - Preguntas metodologicas para la reunion

1. Confirmar si T0 debe ser el primer antibiotico mapeado en +/-48 h o el momento del cultivo positivo. Clinicamente, T0 actual puede ocurrir antes del cultivo.
2. Decidir como tratar pacientes que siguen en UCI tras 30 dias: censura administrativa, fallo de mejoria, o seguimiento extendido.
3. Decidir como clasificar pacientes que mejoran despues de 30 dias: no mejoria a 30 dias frente a censura.
4. Definir muerte como evento competitivo: no basta con censurar si la pregunta es tiempo hasta mejoria.
5. Definir alta de UCI como evento competitivo o resultado favorable. Puede indicar mejoria, pero tambien limita observacion diaria.
6. Validar `sustained_improvement` como curacion/mejoria clinica: dos dias consecutivos con 3/5 dominios, sin nuevo foco y radiologia estable.
7. Definir curacion microbiologica: cultivo negativo, ausencia de cultivos positivos, aclaramiento de bacteriemia, o no medible en MIMIC.
8. Revisar si "sin nueva microbiologia positiva" puede interpretarse como "sin nuevo foco" o si hay sesgo por intensidad de muestreo.
9. Validar lista de antibioticos incluidos/excluidos y filtros de via; armonizar filtros entre T0 y regimen basal.
10. Validar niveles de espectro y dominios de cobertura, especialmente carbapenems, anti-MRSA, aminoglucosidos, polimixinas y nuevas combinaciones.
11. Decidir si se analizaran grupos microorganismo-antibiotico de los prototipos historicos o el esquema actual de espectro basal.
12. Integrar o descartar antibiograma para definir MDR, terapia adecuada y grupos clinicamente relevantes.
13. Confirmar lista de microorganismos incluidos/excluidos; discutir contaminantes, colonizacion y muestras no esteriles.
14. Evaluar sesgo por duracion de estancia: solo quienes sobreviven/permanecen observables pueden mostrar mejoria sostenida.
15. Evaluar immortal time bias: exposiciones definidas por tratamiento o duracion futura pueden requerir modelos tiempo-dependientes.
16. Revisar si variables fisiologicas usadas como outcome son validas en infecciones no respiratorias o en pacientes cronicos.
17. Definir manejo de missingness: fisiologia ausente como no mejoria frente a imputacion o criterio de disponibilidad minima.
18. Decidir si edad, sexo, comorbilidades y gravedad basal son obligatorias para publicacion.
19. Discutir si excluir comorbilidades exploratorias (`N18`, `E11`, `I50`) tiene sentido; actualmente no se excluyen en pipeline final.
20. Determinar si el outcome debe ser tiempo hasta primera mejoria sostenida, proporcion con mejoria a dia 7/14/30, o trayectoria longitudinal.

## Entregable 8 - Esquema para presentacion

| Diapositiva | Titulo | Mensaje principal | Bullets | Figura/tabla sugerida | Nota del ponente |
|---:|---|---|---|---|---|
| 1 | Pregunta clinica | Optimizar duracion antibiotica en infecciones graves requiere medir respuesta diaria. | MIMIC-IV UCI; cultivos positivos; antibioticos; seguimiento longitudinal | Esquema paciente-dia | Enfatizar que es estudio observacional y de generacion de evidencia. |
| 2 | Hipotesis | Algunos pacientes alcanzan mejoria antes de duraciones estandar. | Menor exposicion; menos eventos adversos; stewardship | Diagrama hipotesis exposicion-outcome | Separar hipotesis clinica de definicion operativa. |
| 3 | Fuentes de datos | El pipeline usa microbiologia, UCI, prescripciones, constantes, laboratorio y radiologia. | `microbiologyevents`; `icustays`; `prescriptions`; `chartevents`; `labevents`; notas radiologia | Tabla de fuentes | Aclarar que radiologia depende de disponibilidad de `mimiciv_note`. |
| 4 | Cohorte | Cultivos positivos durante UCI para microorganismos predefinidos. | Dentro de UCI; org list; muestras excluidas; monomicrobiano | Flowchart de inclusion | Decir que edad/comorbilidad no estan en pipeline final. |
| 5 | Episodio indice | Se usa el primer evento microbiologico elegible por estancia UCI. | Orden por `index_charttime`; una fila por `stay_id`; episodios posteriores fuera | Timeline por estancia | Requiere acuerdo clinico si hay reinfecciones. |
| 6 | Definicion de T0 | T0 es inicio del primer antibiotico mapeado en +/-48 h del cultivo. | No necesariamente cultivo; ventana simetrica; excluye oral/enema/flush/dwell | Timeline cultivo-T0 | Punto clave para discusion con tutores. |
| 7 | Regimen basal | Se resume el tratamiento activo en T0 por numero, espectro y farmacos. | `n_abx_t0`; `spectrum_level_t0`; multihot; dominios cobertura | Tabla de niveles de espectro | Advertir que no mide duracion ni desescalada todavia. |
| 8 | Ventanas diarias | Cada paciente aporta dias desde T0 hasta alta UCI, muerte o 30 dias. | `day_idx`; `window_start/end`; censura | Diagrama longitudinal | Introducir alta y muerte como eventos competitivos. |
| 9 | Variables diarias | Se calculan medianas diarias de fisiologia y laboratorio. | Temp; WBC; MAP; lactato; SpO2/FiO2; organos | Heatmap missingness sugerido | Explicar que son proxies de respuesta. |
| 10 | Outcome | Mejoria sostenida = dos dias consecutivos con mejoria diaria. | 3/5 dominios; sin nuevo foco; radiologia estable | Matriz de dominios por dia | Resaltar limitaciones y necesidad de validacion clinica. |
| 11 | Riesgos metodologicos | La interpretacion depende de censura, missingness y sesgos temporales. | muerte; alta UCI; supervivencia; intensidad de medicion; radiologia regex | Tabla de riesgos/QC | Llevar preguntas concretas a la reunion. |
| 12 | Proximos pasos | Cerrar definiciones clinicas antes de modelar/publicar. | arreglar dependencias `_clean`; integrar antibiograma; añadir demografia/gravedad; QC | Checklist | Terminar con decisiones que se necesitan de tutores. |

## Introduccion, hipotesis y objetivos propuestos para presentacion

Introduccion: Las infecciones en UCI por microorganismos multirresistentes o clinicamente relevantes suelen tratarse con pautas prolongadas. Sin embargo, la respuesta clinica puede ser heterogenea y algunos pacientes podrian alcanzar estabilidad antes de completar duraciones estandar.

Hipotesis clinica: En pacientes UCI con cultivo positivo para microorganismos relevantes, una proporcion identificable alcanza mejoria clinica sostenida en los primeros dias tras inicio de tratamiento, lo que podria apoyar estrategias de acortamiento individualizado.

Objetivo primario: Describir y modelar el tiempo hasta mejoria clinica sostenida desde T0 en estancias UCI con infeccion confirmada por cultivo positivo y tratamiento antibiotico mapeado.

Objetivos secundarios: evaluar asociacion entre regimen basal/espectro y mejoria; describir trayectorias fisiologicas; comparar microorganismos y tipos de muestra; cuantificar censura por alta UCI, muerte y 30 dias; explorar integracion futura del antibiograma.

## Variables que faltan o deben revisarse

- Demografia: edad, sexo, raza/etnia.
- Gravedad basal: SOFA, OASIS/SAPS, ventilacion, vasopresores, ingreso quirurgico/medico.
- Comorbilidad: Charlson/Elixhauser o comorbilidades especificas. Los prototipos excluian `N18`, `E11`, `I50`, pero el pipeline final no lo hace.
- Microbiologia avanzada: MDR, ESBL, CRE, MRSA, VRE, antibioterapia adecuada.
- Tratamiento longitudinal: duracion, interrupcion, escalada, desescalada, cambio tras antibiograma.
- Outcomes duros: mortalidad a 30 dias, alta viva, recurrencia, nuevo cultivo positivo, reinfeccion.
- Control de foco: cirugia, drenaje, retirada de cateter; no identificado en codigo.

## Checks de calidad recomendados

1. Conteos por etapa: sujetos, `hadm_id`, `stay_id`, `microevent_id`.
2. Unicidad de claves: `stay_id` en tablas de cohorte/T0/resumen; `stay_id, day_idx` en tablas diarias.
3. Distribucion de `index_charttime - icu_intime` y `true_t0 - index_charttime`.
4. Perdidas por ausencia de antibiotico mapeado.
5. Farmacos no mapeados mas frecuentes en `prescriptions`.
6. Concordancia entre detalle, resumen y multihot del regimen basal.
7. Missingness diario por variable y por `day_idx`.
8. Distribucion de dominios y de `sustained_improvement`.
9. Eventos competitivos: muerte y alta antes de mejoria.
10. Revision manual de muestra de notas radiologicas positivas.
11. Revision manual de eventos `new_foci_events_clean`.
12. Validacion de tablas `_clean` usadas por la tabla final.

