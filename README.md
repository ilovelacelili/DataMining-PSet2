# Proyecto 2 de Data Mining 202520

**Mateo Vivanco (00328476)**

### Introducción


### Descripción y diagrama de arquitectura


# Checklist de aceptación (copiar en el README y marcar)

- [x] Docker Compose levanta Postgres + Mage
- [x] Credenciales en Mage Secrets y .env (solo .env.example en repo)
- [x] Pipeline ingest_bronze mensual e idempotente + tabla de cobertura
- [x] dbt corre dentro de Mage: dbt_build_silver, dbt_build_gold, quality_checks
- [x] Silver materialized = views; Gold materialized = tables
- [x] Gold tiene esquema estrella completo
- [x] Particionamiento: RANGE en fct_trips, HASH en dim_zone, LIST en dim_service_type y dim_payment_type
- [x] README incluye \d+ y EXPLAIN (ANALYZE, BUFFERS) con pruning
- [x] dbt test pasa desde Mage
- [x] Notebook responde 20 preguntas usando solo gold.*
- [x] Triggers configurados y evidenciados