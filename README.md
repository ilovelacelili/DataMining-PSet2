# Proyecto 2 de Data Mining 202520

**Mateo Vivanco (00328476)**

### Introducción


### Descripción y diagrama de arquitectura


# Checklist de aceptación (copiar en el README y marcar)
- [x] Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green; matriz de
cobertura en README. 
- [x] Mage orquesta backfill mensual con idempotencia y metadatos por lote.
- [x] Bronze (raw) refleja fielmente el origen; Silver unifica/escaliza; Gold en estrella con
fct_trips y dimensiones clave.
- [x] Clustering aplicado a fct_trips con evidencia antes/después (Query Profile,
pruning)
- [x] Secrets y cuenta de servicio con permisos mínimos (evidencias sin exponer valores).
- [x] Tests dbt (not_null, unique accepted_values, relationships) pasan; docs y lineage generados.
- [x] Notebook con respuestas a las 5 preguntas de negocio desde gold.
