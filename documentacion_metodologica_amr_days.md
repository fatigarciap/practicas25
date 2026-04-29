# 1. Estado actual del proyecto

El proyecto `amr_days / MIMIC-IV` esta en fase de construccion y validacion de una tabla longitudinal diaria. El objetivo actual no es modelar todavia, sino confirmar que la cohorte, el episodio indice, T0, las ventanas diarias, las variables clinicas y el outcome estan correctamente definidos antes de pasar a analisis de supervivencia, competing risks o machine learning.

**Implementado en codigo:** existe un pipeline canonico en `analysis/pipeline_final/` que materializa tablas intermedias y una tabla final llamada `longitudinal_cohort_model_ready`. La unidad final es `stay_id`-dia.

**Inferido por estructura del pipeline:** la tabla esta pensada para estudiar trayectorias clinicas desde el inicio del tratamiento antibiotico relevante hasta mejoria, muerte, alta de UCI o 30 dias.

**Pendiente de validacion clinica:** confirmar si las definiciones operativas actuales representan infeccion real, tratamiento relevante y mejoria clinica suficientemente validos para presentacion y modelado.

Punto tecnico critico: el script final `06_01_longitudinal_cohort_model_ready.sql` referencia `clinical_domains_sci` e `improvement_flags`, pero los scripts canonicos crean `clinical_domains_sci_clean` e `improvement_flags_clean`. Esto debe corregirse o documentarse como alias antes de validar resultados.

# 2. Pregunta clinica

Pregunta principal:

> Podemos identificar cuando los pacientes UCI con infeccion microbiologicamente confirmada alcanzan mejoria clinica sostenida durante el tratamiento antibiotico?

La pregunta esta orientada a describir y validar el tiempo hasta mejoria clinica sostenida, no todavia a inferir causalidad ni a recomendar duraciones antibioticas.

**Implementado en codigo:** se crea un seguimiento diario desde T0 y se define `sustained_improvement`.

**Pendiente de validacion clinica:** si `sustained_improvement` es un proxy aceptable de mejoria clinica y si debe complementarse con curacion microbiologica, mortalidad, alta UCI o control de foco.

# 3. Hipotesis

Hipotesis clinica:

En pacientes ingresados en UCI con cultivos positivos para microorganismos clinicamente relevantes, una parte de los pacientes alcanza estabilidad o mejoria clinica sostenida durante los primeros dias tras el inicio del tratamiento antibiotico.

Hipotesis metodologica:

Una tabla longitudinal diaria construida desde MIMIC-IV puede capturar trayectorias de respuesta clinica usando variables fisiologicas, analiticas, microbiologicas y radiologicas agregadas por dia.

**Implementado en codigo:** la respuesta se aproxima con temperatura, leucocitos, MAP, lactato, SpO2/FiO2, ausencia de nuevo foco y estabilidad radiologica.

**Pendiente de validacion clinica:** los dominios, umbrales, manejo de datos faltantes y definicion de mejoria sostenida requieren aprobacion antes del modelado.

# 4. Construccion de la cohorte

## Poblacion fuente

**Implementado en codigo:** estancias UCI de MIMIC-IV (`physionet-data.mimiciv_3_1_icu.icustays`) con eventos microbiologicos de hospital (`physionet-data.mimiciv_3_1_hosp.microbiologyevents`) unidos por `hadm_id`.

El cultivo debe tener `charttime` dentro de la estancia UCI:

```text
micro.charttime BETWEEN icu.intime AND icu.outtime
```

## Criterios de inclusion

**Implementado en codigo:**

- Estancia UCI con `stay_id`.
- Cultivo positivo durante UCI.
- `micro.charttime` no nulo.
- `micro.hadm_id` no nulo.
- `micro.org_name` no nulo.
- Microorganismo dentro de una lista cerrada.
- Tipo de muestra no excluido.
- Evento monomicrobiano.
- Al menos un antibiotico mapeado iniciado entre 48 h antes y 48 h despues del cultivo indice.
- T0 anterior al fin de seguimiento posible.

## Criterios de exclusion

**Implementado en codigo:**

- Cultivos fuera de la estancia UCI.
- Cultivos sin microorganismo identificado.
- Microorganismos fuera de la lista predefinida.
- Tipos de muestra excluidos.
- Eventos polimicrobianos segun la definicion del pipeline.
- Estancias sin antibiotico mapeado en la ventana alrededor del cultivo.
- Estancias con `t0 >= followup_end`.

**No implementado en pipeline final:** exclusion por edad, sexo, comorbilidad, inmunosupresion, diagnostico de sepsis, ventilacion mecanica, shock, limitacion terapeutica, primera estancia por paciente o gravedad basal.

**Inferido por scripts exploratorios, no canonico:** algunos prototipos exploraron exclusion por codigos ICD de insuficiencia renal cronica, diabetes e insuficiencia cardiaca (`N18`, `E11`, `I50`), pero esto no esta aplicado en el pipeline final.

## Filtros microbiologicos

Microorganismos incluidos:

- `escherichia coli`
- `klebsiella pneumoniae`
- `klebsiella aerogenes`
- `enterobacter cloacae`
- `enterobacter aerogenes`
- `pseudomonas aeruginosa`
- `acinetobacter baumannii`
- `stenotrophomonas maltophilia`
- `enterococcus faecium`
- `staphylococcus aureus`

**Pendiente de validacion clinica:** el codigo no distingue MDR, ESBL, CRE, MRSA, VRE ni resistencia especifica. La inclusion actual es por especie, no por fenotipo de resistencia.

## Tipos de muestra

**Implementado en codigo:** se excluyen los siguientes `spec_type_desc` tras convertirlos a minusculas:

- `swab`
- `fluid,other`
- `foreign body`
- `foot culture`
- `fluid received in blood culture bottles`
- `ear`
- `fluid wound`
- `dialysis fluid`
- `skin scrapings`
- `foreign body - sonication culture`
- `eye`

**Implementado por omision:** los tipos de muestra no incluidos en esa lista quedan permitidos si cumplen el resto de filtros.

**Pendiente de validacion clinica:** confirmar si esa lista excluye correctamente colonizacion/contaminacion y si deben tratarse de forma distinta muestras respiratorias, sangre, orina, cateteres u otros focos.

## Filtros temporales

**Implementado en codigo:**

- Cultivo indice dentro de UCI.
- Antibiotico relevante iniciado entre `index_charttime - 48 h` e `index_charttime + 48 h`.
- Seguimiento desde T0 hasta el minimo de alta UCI, muerte o T0 + 30 dias.

## Filtros antibioticos

**Implementado en codigo para definir T0:**

- `prescriptions.drug` no nulo.
- `prescriptions.starttime` no nulo.
- Nombre del farmaco debe hacer match con `abx_spectrum_map_clean`.
- Se excluyen nombres que contienen `oral`, `enema`, `flush` o `dwell`.

**Riesgo implementado:** esos filtros de exclusion de via/texto se aplican al identificar T0, pero no se repiten en `baseline_regimen_detail_clean`.

# 5. Episodio indice

## Que es

El episodio indice es el primer cultivo positivo elegible dentro de una estancia UCI que cumple los criterios microbiologicos, temporales y de monomicrobialidad.

## Como se selecciona

**Implementado en codigo:**

1. `00_01_episode_candidates_clean.sql` genera candidatos de episodio.
2. `00_03_index_stay_clean.sql` aplica:

```sql
ROW_NUMBER() OVER (
  PARTITION BY stay_id
  ORDER BY index_charttime ASC, microevent_id ASC
) AS rn
```

3. Se conserva solo `rn = 1`.

El resultado es `bloque_0b_index_stay_clean`, con una fila por `stay_id`.

## Por que se usa el primer cultivo positivo elegible

**Inferido por estructura del pipeline:** usar el primer cultivo elegible por estancia simplifica la definicion de cohorte, evita multiples episodios correlacionados dentro de la misma estancia UCI y crea un unico punto de anclaje para T0 y seguimiento.

## Ventajas metodologicas

- Una unidad clara: una estancia UCI aporta como maximo un episodio.
- Reduce dependencia entre eventos repetidos.
- Facilita trazabilidad de T0, ventanas y outcome.
- Evita mezclar respuesta a episodios sucesivos.

## Limitaciones

- Excluye reinfecciones o nuevos episodios durante la misma estancia.
- Puede ignorar un episodio posterior mas grave o mas clinicamente relevante.
- No garantiza que el primer cultivo positivo sea el verdadero foco clinico principal.
- Requiere validacion si hay multiples cultivos cercanos o cultivos de vigilancia.

# 6. T0

## Definicion operativa exacta segun codigo

**Implementado en codigo:** T0 se define en `01_01_t0_true.sql` como:

```text
true_t0 = MIN(start_ts)
```

entre los antibioticos mapeados iniciados dentro de:

```text
index_charttime - 48 h <= start_ts <= index_charttime + 48 h
```

Por tanto, T0 no es el cultivo. T0 es el primer inicio de antibiotico relevante en la ventana alrededor del cultivo indice.

## Relacion con `index_charttime`

`index_charttime` es el `micro.charttime` del cultivo indice. T0 se busca alrededor de ese tiempo.

**Implementado en codigo:** T0 puede ser anterior, igual o posterior al cultivo indice.

## Ventana +/-48 h

**Implementado en codigo:** la ventana usada para detectar antibiotico relevante es simetrica:

- desde 48 h antes del cultivo indice
- hasta 48 h despues del cultivo indice

## Captura tratamiento empirico previo?

Si. **Implementado en codigo:** un antibiotico iniciado antes del cultivo, pero dentro de las 48 h previas, puede ser seleccionado como T0.

## Que significa clinicamente

**Inferido por estructura del pipeline:** T0 representa el inicio del tratamiento antibiotico relevante asociado al episodio microbiologico indice, incluyendo tratamiento empirico iniciado antes de obtener el cultivo.

## Riesgos o limitaciones

- T0 puede preceder al cultivo, por lo que el dia 0 puede representar tratamiento empirico previo a la confirmacion microbiologica.
- Si el antibiotico relevante empieza antes del cultivo, la respuesta observada desde T0 no es "respuesta tras cultivo positivo", sino "respuesta desde inicio terapeutico asociado al cultivo".
- La ventana de 48 h es una decision metodologica que requiere confirmacion.
- Si un antibiotico no esta en el mapa, el paciente puede quedar fuera aunque haya recibido tratamiento.
- Los antibioticos excluidos por texto (`oral`, `flush`, etc.) pueden depender de como este escrito `drug`.

# 7. Regimen antibiotico basal

## Que antibioticos entran

**Implementado en codigo:** entran antibioticos de `prescriptions` que:

- pertenecen al mismo `hadm_id`,
- estan activos en T0,
- hacen match con algun patron de `abx_spectrum_map_clean`.

Un antibiotico se considera activo en T0 si:

```text
start_ts <= true_t0
AND (stop_ts IS NULL OR stop_ts > true_t0)
```

## Antibioticos iniciados antes del cultivo pero activos en T0

Si. **Implementado en codigo:** pueden incluirse si estan activos en `true_t0`. De hecho, si T0 fue un antibiotico iniciado antes del cultivo, ese antibiotico forma parte del regimen basal.

## Como se estandarizan nombres

**Implementado en codigo:** `abx_spectrum_map_clean` usa expresiones regulares sobre `LOWER(p.drug)` para asignar:

- `abx_name_std`
- `spectrum_level`
- `spectrum_label`
- `coverage_domain`
- `match_priority`

Si un nombre coincide con varios patrones, se conserva el de menor `match_priority`.

## Antibioticos considerados relevantes

**Implementado en codigo:** el mapa incluye, entre otros:

- penicilinas y combinaciones: `ampicillin`, `amoxicillin`, `ampicillin_sulbactam`, `amoxicillin_clavulanate`, `pip_tazo`
- cefalosporinas: `cefazolin`, `cefuroxime`, `ceftriaxone`, `cefotaxime`, `cefepime`, `ceftaroline`
- nuevos beta-lactamicos/inhibidores: `ceftazidime_avibactam`, `ceftolozane_tazobactam`, `meropenem_vaborbactam`
- carbapenems: `meropenem`, `imipenem`, `ertapenem`
- quinolonas: `ciprofloxacin`, `levofloxacin`
- aminoglucosidos: `gentamicin`, `tobramycin`, `amikacin`
- anaerobios/otros: `metronidazole`, `clindamycin`, `aztreonam`, `tmp_smx`
- anti-MRSA/VRE o grampositivos resistentes: `vancomycin_iv`, `teicoplanin`, `linezolid`, `daptomycin`
- reserva GN MDR: `colistin`, `polymyxin_b`, `tigecycline`, `cefiderocol`, `fosfomycin_iv`

**Pendiente de validacion clinica:** confirmar si la lista es completa, si faltan farmacos usados en MIMIC y si algunos deben excluirse por via, indicacion o baja relevancia para infeccion sistemica.

## Clasificacion por espectro

**Implementado en codigo:**

- `1`: estrecho
- `2`: intermedio
- `3`: amplio
- `4`: muy amplio

Tambien se asigna `coverage_domain`, por ejemplo `general`, `anaerobios`, `general_reserva`, `gn_multirresistente`, `gp_resistente`.

**Pendiente de validacion clinica:** el espectro es una clasificacion fija por farmaco, no dependiente del microorganismo ni del antibiograma. No equivale automaticamente a tratamiento adecuado.

## Variables creadas

Detalle basal (`baseline_regimen_detail_clean`):

- `abx_name_std`
- `spectrum_level`
- `spectrum_label`
- `coverage_domain`
- `start_ts`
- `stop_ts`

Resumen basal (`baseline_regimen_summary_clean`):

- `n_abx_t0`
- `spectrum_level_t0`
- `has_broad_t0`
- `has_gp_resistant_t0`
- `has_gn_mdr_t0`

Multihot basal (`baseline_regimen_multihot_clean`):

- `t0_vancomycin`
- `t0_pip_tazo`
- `t0_cefepime`
- `t0_meropenem`
- `t0_ceftriaxone`
- `t0_ciprofloxacin`
- `t0_linezolid`
- `t0_daptomycin`
- `t0_cefazolin`
- `t0_levofloxacin`
- `t0_ampicillin`
- `t0_ampicillin_sulbactam`
- `t0_metronidazole`
- `t0_tobramycin`
- `t0_gentamicin`
- `t0_aztreonam`
- `t0_clindamycin`
- `t0_any_aminoglycoside`

## Interpretacion clinica

El regimen basal resume la intensidad y composicion del tratamiento en T0. Actualmente describe exposicion antibiotica inicial, pero no duracion total, adecuacion al antibiograma, escalada, desescalada ni cambios posteriores.

# 8. Tabla longitudinal

## Unidad de analisis

**Implementado en codigo:** una fila por `stay_id` y `day_idx`.

La tabla final es `longitudinal_cohort_model_ready`.

## Dia 0, dia 1, dia 2

**Implementado en codigo:**

- `day_idx = 0`: primera ventana desde T0.
- `day_idx = 1`: 24-48 h despues de T0.
- `day_idx = 2`: 48-72 h despues de T0.

Cada ventana se define con:

```text
window_start = t0 + day_idx * 24 h
window_end = min(t0 + (day_idx + 1) * 24 h, followup_end)
```

## Variables clinicas y analiticas diarias

**Implementado en codigo:** `daily_features_clean` agrega medianas diarias aproximadas usando `APPROX_QUANTILES`.

Variables vitales:

- `HR_median`
- `MAP_median`
- `SysBP_median`
- `DiasBP_median`
- `Temp_median`
- `RR_median`
- `SpO2_median`
- `FiO2_median`

Variables analiticas:

- `WBC_median`
- `Lactate_median`
- `Creatinine_median`
- `Bilirubin_median`
- `Platelets_median`
- `Hgb_median`

Variable derivada:

- `spo2fio2_ratio = SpO2_median / FiO2_median`

## Seguimiento

**Implementado en codigo:** cada estancia se sigue desde T0 hasta:

```text
followup_end = LEAST(icu_outtime, deathtime si existe, t0 + 30 dias)
```

Por tanto, el seguimiento termina por:

- alta de UCI,
- muerte,
- censura administrativa a 30 dias.

**Pendiente de validacion clinica:** muerte y alta UCI estan usadas como fin de observacion, pero no estan modeladas todavia como eventos competitivos.

# 9. Outcome

## `improved_today`

**Implementado en codigo:** `improved_today = 1` si se cumplen todas estas condiciones:

```text
COALESCE(no_new_foci_flag, 1) = 1
AND COALESCE(radiology_stable_flag, 1) = 1
AND n_domains_ok >= 3
```

`n_domains_ok` suma cinco dominios fisiologicos/laboratorio:

- `temp_in_range`: temperatura entre 36 y 38 C.
- `wbc_normalizing`: leucocitos entre 4 y 12.
- `hemo_stable`: MAP >= 65.
- `lactate_normalizing`: lactato < 2.
- `resp_improving`: SpO2/FiO2 >= 240.

Los nulos de estos cinco dominios se cuentan como 0 al construir `n_domains_ok`.

## `sustained_improvement`

**Implementado en codigo:** `sustained_improvement = 1` si:

```text
improved_today = 1
AND improved_today del dia anterior = 1
```

Se calcula con `LAG(improved_today)` por `stay_id` ordenado por `day_idx`.

## Dominios no fisiologicos

Ausencia de nuevo foco:

- `new_foci_events_clean` marca eventos microbiologicos posteriores si aparece muestra de sangre, tipo de muestra distinto o microorganismo distinto.
- `new_foci_flag_clean` genera `no_new_foci_flag`, que vale 1 antes del primer nuevo foco y 0 desde el primer nuevo foco.

Estabilidad radiologica:

- `radiology_worsening_events_clean` busca terminos de empeoramiento en notas de radiologia.
- `radiology_flag_clean` genera `radiology_stable_flag`, que vale 1 antes del primer empeoramiento y 0 desde el primer empeoramiento.

## Necesidad de validacion clinica

**Pendiente de validacion clinica:**

- si 3 de 5 dominios es el umbral correcto;
- si todos los dominios deben pesar igual;
- si ausencia de radiologia positiva equivale a estabilidad;
- si ausencia de nuevo foco equivale a curacion microbiologica;
- si SpO2/FiO2 >= 240 es adecuado para todos los focos;
- si missing fisiologico debe contar como no mejoria;
- si dos dias consecutivos son suficientes para hablar de mejoria sostenida.

# 10. Variables para modelado futuro

## Exposicion principal

**Disponible actualmente:**

- regimen antibiotico basal en T0;
- `n_abx_t0`;
- `spectrum_level_t0`;
- `has_broad_t0`;
- `has_gp_resistant_t0`;
- `has_gn_mdr_t0`;
- indicadores multihot de antibioticos en T0.

**No disponible todavia:** duracion total de tratamiento, DOT, escalada, desescalada, cambios de regimen, adecuacion al antibiograma.

## Outcome

**Disponible actualmente:**

- `improved_today`;
- `sustained_improvement`;
- primer dia de `sustained_improvement` derivable por `MIN(day_idx)`;
- pacientes sin mejoria derivables por ausencia de `sustained_improvement`.

## Predictoras basales

**Disponibles:**

- `subject_id`;
- `hadm_id`;
- `stay_id`;
- `organism_name`;
- `specimen_type`;
- `index_charttime`;
- `t0`;
- `icu_intime`;
- `icu_outtime`;
- `deathtime`;
- variables de regimen basal.

**Faltan o requieren construccion:**

- edad;
- sexo;
- raza/etnia;
- comorbilidad;
- SOFA, OASIS, SAPS u otra gravedad basal;
- ventilacion mecanica;
- vasopresores;
- foco clinico;
- control de foco.

## Predictoras longitudinales

**Disponibles:**

- `day_idx`;
- medianas diarias de constantes;
- medianas diarias de laboratorio;
- `spo2fio2_ratio`;
- dominios clinicos;
- flags diarios de radiologia y nuevo foco.

## Confundidoras

**Disponibles como proxies:**

- MAP;
- lactato;
- SpO2/FiO2;
- creatinina;
- bilirrubina;
- plaquetas;
- tipo de muestra;
- microorganismo;
- tiempo desde ingreso UCI hasta cultivo, derivable de `icu_intime` e `index_charttime`.

**Faltan de forma importante:**

- edad;
- sexo;
- comorbilidad;
- gravedad basal validada;
- soporte organico;
- foco de infeccion;
- adecuacion antibiotica;
- limitacion terapeutica.

## Eventos competitivos

**Disponibles como tiempos de fin de seguimiento:**

- `deathtime`;
- `icu_outtime`;
- censura a T0 + 30 dias.

**No implementado todavia como analisis:** competing risks para muerte y alta UCI.

# 11. Que falta validar antes de modelar

1. Corregir o confirmar las referencias del script final a `clinical_domains_sci` e `improvement_flags` sin sufijo `_clean`.
2. Ejecutar QC de unicidad: una fila por `stay_id` en episodio indice y una fila por `stay_id, day_idx` en tabla final.
3. Confirmar que T0 debe ser el primer antibiotico mapeado en +/-48 h del cultivo, y no el cultivo en si.
4. Validar clinicamente la ventana +/-48 h y si debe incluir tratamiento empirico previo.
5. Revisar la lista de microorganismos y decidir si el estudio es por especie o por fenotipo de resistencia.
6. Integrar o descartar el antibiograma para definir MDR, terapia adecuada, MRSA, VRE, ESBL o CRE.
7. Validar tipos de muestra incluidos/excluidos y riesgo de colonizacion/contaminacion.
8. Armonizar filtros de antibioticos entre definicion de T0 y regimen basal.
9. Revisar el mapa de antibioticos, patrones regex, niveles de espectro y farmacos faltantes.
10. Definir si la exposicion principal sera espectro basal, antibiotico especifico, combinacion, duracion o adecuacion.
11. Construir duracion antibiotica antes de responder preguntas sobre tratamientos cortos.
12. Validar `improved_today` y `sustained_improvement` como outcomes clinicos.
13. Decidir manejo de missingness en variables fisiologicas.
14. Validar el uso de radiologia por regex y ausencia de nuevo foco como proxies de evolucion.
15. Decidir si muerte y alta UCI seran eventos competitivos en el analisis principal.
16. Construir variables basales minimas: edad, sexo, comorbilidad y gravedad.
17. Evaluar sesgos por supervivencia, duracion de estancia, intensidad de medicion e immortal time bias.
18. Definir si pacientes que siguen en UCI tras 30 dias son censurados, no respondedores o grupo especial.

# 12. Como explicarlo en presentacion

## Slide 1 - Resumen del pipeline

**Mensaje principal:** Hemos construido una tabla longitudinal diaria desde MIMIC-IV para validar trayectorias de respuesta clinica en pacientes UCI con cultivo positivo y tratamiento antibiotico asociado.

**Bullets:**

- MIMIC-IV: UCI, microbiologia, prescripciones, constantes, laboratorio y radiologia.
- Cohorte: cultivo positivo elegible durante UCI.
- T0: primer antibiotico mapeado en +/-48 h del cultivo.
- Seguimiento diario hasta muerte, alta UCI o 30 dias.
- Outcome: mejoria clinica diaria y sostenida.

**Figura sugerida:**

```text
icustays + microbiology -> episodio indice -> T0 antibiotico
                                      -> ventanas diarias
                                      -> variables clinicas
                                      -> outcome longitudinal
```

**Nota del ponente:** El objetivo de esta fase es validar la tabla, no presentar aun un modelo predictivo.

## Slide 2 - Cohorte

**Mensaje principal:** La cohorte se define por estancias UCI con primer cultivo positivo elegible, monomicrobiano, para microorganismos predefinidos.

**Bullets:**

- Cultivo dentro de `icu_intime` y `icu_outtime`.
- Lista cerrada de 10 microorganismos.
- Exclusion de tipos de muestra potencialmente no representativos.
- Un episodio indice por `stay_id`.
- Entrada final requiere antibiotico mapeado cerca del cultivo.

**Figura sugerida:** flowchart de inclusion/exclusion por etapas.

**Nota del ponente:** Edad, comorbilidad y gravedad no estan aun incorporadas en el pipeline final.

## Slide 3 - T0

**Mensaje principal:** T0 es terapeutico, no microbiologico: corresponde al primer antibiotico relevante iniciado en una ventana de +/-48 h alrededor del cultivo indice.

**Bullets:**

- `index_charttime`: hora del cultivo indice.
- `true_t0`: primer inicio de antibiotico mapeado en +/-48 h.
- Puede ocurrir antes del cultivo.
- Captura tratamiento empirico previo.
- Requiere validacion clinica.

**Figura sugerida:**

```text
index_charttime -48h        cultivo indice        index_charttime +48h
          |----------------------|----------------------|
                   T0 puede caer aqui
```

**Nota del ponente:** Esta es una decision central porque define el dia 0 y todas las ventanas posteriores.

## Slide 4 - Outcome

**Mensaje principal:** La mejoria sostenida se define como dos dias consecutivos con mejoria diaria.

**Bullets:**

- `improved_today`: sin nuevo foco, radiologia estable y al menos 3/5 dominios clinicos.
- Dominios: temperatura, leucocitos, MAP, lactato, SpO2/FiO2.
- `sustained_improvement`: `improved_today` actual y previo.
- Missing y proxies requieren validacion.

**Figura sugerida:** matriz paciente-dia con dominios marcados y dia de primera mejoria sostenida.

**Nota del ponente:** Este outcome es operacional y necesita acuerdo clinico antes de usarse como endpoint principal.

## Slide 5 - Siguientes pasos hacia modelado

**Mensaje principal:** Antes de modelar, hay que cerrar definiciones clinicas, validar calidad de datos y construir variables de confusores y eventos competitivos.

**Bullets:**

- Ejecutar QC de duplicados, coherencia temporal, missingness y outcome.
- Corregir dependencias `_clean` de la tabla final.
- Validar T0, microorganismos, muestras y antibioticos.
- Anadir edad, sexo, comorbilidad, gravedad y soporte organico.
- Preparar analisis de supervivencia y competing risks.

**Figura sugerida:** checklist de validacion antes de modelado.

**Nota del ponente:** La tabla longitudinal es la base; el modelado vendra despues de validar que las definiciones son clinicamente defendibles.

