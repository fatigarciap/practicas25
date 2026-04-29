# Project Structure Proposal

Fecha: 2026-04-28  
Base: `analysis/PIPELINE_AUDIT.md`  
Alcance: propuesta de organizacion y clasificacion. Los scripts existentes no se han modificado ni movido.

## 1. Estructura final propuesta

Estructura canonica recomendada:

```text
analysis/
  PIPELINE_AUDIT.md
  PROJECT_STRUCTURE.md
  README.md                         # PENDIENTE

  sql/
    README.md
    cohort/
    antibiotics/
    windows/
    daily_features/
    outcome/
    final_table/
    qc/

  pipeline_final/                   # fuente canonica actual; mantener hasta migracion validada
    00_cohort/
    01_antibiotics_t0/
    02_windows/
    03_events/
    04_daily_features/
    05_outcome/
    06_final_table/

  windows/                          # legacy/prototipo; no canonico

  archive_exploratory/              # PENDIENTE, no creado aun para evitar mover scripts
    sql/
    windows/
```

Uso recomendado:

- `analysis/pipeline_final/`: scripts canonicos actuales, ejecutables en orden.
- `analysis/sql/<dominio>/`: estructura final propuesta para una migracion posterior controlada.
- `analysis/sql/qc/`: scripts reconstruidos de control de calidad, actualmente placeholders.
- `analysis/sql/*.sql` y `analysis/windows/*.sql`: consultas historicas o exploratorias; conservar por trazabilidad hasta decidir archivo.

## 2. Clasificacion de scripts SQL existentes

### cohort

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/00_cohort/00_01_episode_candidates_clean.sql` | cohort | canonico |
| `analysis/pipeline_final/00_cohort/00_02_antibiogram_detail_clean.sql` | cohort | canonico auxiliar; no consumido despues |
| `analysis/pipeline_final/00_cohort/00_03_index_stay_clean.sql` | cohort | canonico |
| `analysis/sql/estancias_uci.sql` | cohort | exploratorio |
| `analysis/sql/recuento_estancias_uci.sql` | cohort | exploratorio/qc descriptivo |
| `analysis/sql/estancias_uci_cultivos.sql` | cohort | exploratorio |
| `analysis/sql/estancias_uci_detalle.sql` | cohort | exploratorio |
| `analysis/sql/estancias_uci_microevents.sql` | cohort | exploratorio |
| `analysis/sql/estancias_uci_monoinfeccion.sql` | cohort | exploratorio |
| `analysis/sql/estancias_uci_monoinfeccion_filtrada.sql` | cohort | exploratorio |
| `analysis/sql/estancias_uci_monoinfeccion_filtrada_comorb.sql` | cohort | exploratorio con comorbilidad |
| `analysis/sql/conteo_estancias_uci_monoinfeccion.sql` | cohort | exploratorio/qc descriptivo |

### antibiotics

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/01_antibiotics_t0/01_01_abx_spectrum_map_clean.sql` | antibiotics | canonico |
| `analysis/pipeline_final/01_antibiotics_t0/01_01_t0_true.sql` | antibiotics | canonico |
| `analysis/pipeline_final/01_antibiotics_t0/01_03_baseline_regimen_detail_clean.sql` | antibiotics | canonico |
| `analysis/pipeline_final/01_antibiotics_t0/01_04_baseline_regimen_summary_clean.sql` | antibiotics | canonico |
| `analysis/pipeline_final/01_antibiotics_t0/01_05_baseline_regimen_multihot_clean.sql` | antibiotics | canonico |
| `analysis/sql/estancias_uci_48h_tratamiento.sql` | antibiotics | exploratorio |
| `analysis/sql/estancias_uci_monoinfeccion_con_tratamiento_previo.sql` | antibiotics | exploratorio |
| `analysis/sql/estancias_uci_bloques_monomicrobianos.sql` | antibiotics | exploratorio |
| `analysis/sql/estancias_uci_metrica_clinica.sql` | antibiotics | exploratorio con variables clinicas y tratamiento |

### windows

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/02_windows/02_01_base_windows_clean.sql` | windows | canonico |
| `analysis/windows/ventana.sql` | windows | prototipo monolitico |

### daily_features

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/04_daily_features/04_01_daily_features_clean.sql` | daily_features | canonico |
| `analysis/windows/itemid_caracteristicas_mejoria.sql` | daily_features | auxiliar exploratorio de itemids |
| `analysis/windows/ventana.sql` | daily_features | prototipo monolitico; tambien clasificado en windows |

### outcome

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/03_events/03_01_radiology_worsening_events_clean.sql` | outcome | canonico, subdominio eventos |
| `analysis/pipeline_final/03_events/03_02_radiology_flag_clean.sql` | outcome | canonico, subdominio eventos |
| `analysis/pipeline_final/03_events/03_03_new_foci_events_clean.sql` | outcome | canonico, subdominio eventos |
| `analysis/pipeline_final/03_events/03_04_new_foci_flag_clean.sql` | outcome | canonico, subdominio eventos |
| `analysis/pipeline_final/05_outcome/05_01_clinical_domains_sci_clean.sql` | outcome | canonico |
| `analysis/pipeline_final/05_outcome/05_02_improvement_flags_clean.sql` | outcome | canonico |

### final_table

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/pipeline_final/06_final_table/06_01_longitudinal_cohort_model_ready.sql` | final_table | canonico con incoherencia de dependencias no clean |

### qc

| Script existente | Clasificacion | Estado |
|---|---|---|
| `analysis/sql/recuento_estancias_uci.sql` | qc | exploratorio/descriptivo |
| `analysis/sql/conteo_estancias_uci_monoinfeccion.sql` | qc | exploratorio/descriptivo |

No hay scripts QC canonicos para el pipeline final. Se crearon placeholders reconstruidos bajo `analysis/sql/qc/`.

## 3. Scripts faltantes detectados

Placeholders creados:

| Placeholder | Motivo |
|---|---|
| `analysis/sql/antibiotics/01_02_antibiotic_exclusion_rules_reconstructed.sql` | Hueco numerico entre `01_01` y `01_03`; documenta reglas de exclusion/filtro de antibioticos a consolidar |
| `analysis/sql/outcome/05_03_clean_aliases_for_final_table_reconstructed.sql` | El script final referencia `clinical_domains_sci` e `improvement_flags` sin `_clean` |
| `analysis/sql/qc/00_qc_cohort_counts_reconstructed.sql` | Falta QC canonico de conteos de cohorte |
| `analysis/sql/qc/01_qc_antibiotics_t0_reconstructed.sql` | Falta QC canonico de t0 y regimen basal |
| `analysis/sql/qc/02_qc_windows_daily_features_reconstructed.sql` | Falta QC canonico de ventanas y variables diarias |
| `analysis/sql/qc/03_qc_outcome_final_table_reconstructed.sql` | Falta QC canonico de outcome y tabla final |

PENDIENTE, no creado como placeholder ejecutable:

- `b0_cohorte.sql`
- `b1_base_windows.sql`
- `b2_dailyfeatures.sql`
- `b2_subnew_foci_flag.sql`
- `b3_clinicaldomains.sql`
- `b4_improvementsflags.sql`
- `pipeline_final/longitudinal_cohort.sql`

Estos nombres aparecen en `queries_bigquery.py`, pero no forman parte del pipeline canonico auditado. Crear archivos con esos nombres podria hacer que funciones legacy ejecuten SQL incompleto por accidente.

## 4. Estructura clara por dominio

```text
analysis/sql/cohort/
  # destino futuro de:
  # 00_01_episode_candidates_clean.sql
  # 00_02_antibiogram_detail_clean.sql
  # 00_03_index_stay_clean.sql

analysis/sql/antibiotics/
  01_02_antibiotic_exclusion_rules_reconstructed.sql
  # destino futuro de:
  # 01_01_abx_spectrum_map_clean.sql
  # 01_01_t0_true.sql
  # 01_03_baseline_regimen_detail_clean.sql
  # 01_04_baseline_regimen_summary_clean.sql
  # 01_05_baseline_regimen_multihot_clean.sql

analysis/sql/windows/
  # destino futuro de:
  # 02_01_base_windows_clean.sql

analysis/sql/daily_features/
  # destino futuro de:
  # 04_01_daily_features_clean.sql

analysis/sql/outcome/
  05_03_clean_aliases_for_final_table_reconstructed.sql
  # destino futuro de:
  # 03_01_radiology_worsening_events_clean.sql
  # 03_02_radiology_flag_clean.sql
  # 03_03_new_foci_events_clean.sql
  # 03_04_new_foci_flag_clean.sql
  # 05_01_clinical_domains_sci_clean.sql
  # 05_02_improvement_flags_clean.sql

analysis/sql/final_table/
  # destino futuro de:
  # 06_01_longitudinal_cohort_model_ready.sql

analysis/sql/qc/
  00_qc_cohort_counts_reconstructed.sql
  01_qc_antibiotics_t0_reconstructed.sql
  02_qc_windows_daily_features_reconstructed.sql
  03_qc_outcome_final_table_reconstructed.sql
```

## 5. Orden recomendado de migracion

1. Mantener `analysis/pipeline_final/` como fuente canonica mientras se valida.
2. Corregir la incoherencia `_clean` vs no clean en el paso final.
3. Implementar QC canonico en `analysis/sql/qc/`.
4. Migrar scripts por dominio a `analysis/sql/<dominio>/` mediante copia controlada, no movimiento inmediato.
5. Archivar scripts exploratorios en `analysis/archive_exploratory/` solo cuando exista confirmacion de que no son dependencias activas.
