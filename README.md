# Proyecto 2 de Data Mining 202520

**Mateo Vivanco (00328476)**

### Introducción
Este proyecto implementa un pipeline end-to-end automatizado con Mage AI, transformando los datos del NYC Taxi & Limousine Commission (Yellow y Green) mediante una Arquitectura Medallion. Para ello, se utilizará Docker Compose, donde se ejecuta el servicio de base de datos PostgreSQL, PGAdmin para visualización y control, y MageAI para manejar la arquitectura y procesamiento. Finalmente, se plantea documentar todo el proceso y responer a 20 preguntas de negocio utilizando la arquitectura de capa Gold.

### Descripción y diagrama de arquitectura

```text
[Fuente: NYC TLC Parquet Files] 
       │
       ▼
(Mage Pipeline: ingest_bronze) 
       │
       ▼
[BRONZE LAYER: PostgreSQL] ➔ Tablas raw con metadatos de ingesta (ingest_ts, source_month)
       │
       ▼
(Mage Pipeline: dbt_build_silver) 
       │
       ▼
[SILVER LAYER: Vistas Curadas] ➔ Limpieza, estandarización y filtrado de calidad
       │
       ▼
(Mage Pipeline: dbt_build_gold) 
       │
       ▼
[GOLD LAYER: Modelo Estrella] ➔ Tablas particionadas optimizadas (Hechos y Dimensiones)
       │
       ▼
[Análisis y Reporting] ➔ Jupyter Notebook (data_analysis.ipynb)
```

## Tabla de Cobertura en capa RAW
|year_month|service_type|status|total_count|
|-|-|-|-|
|2022-01|green|loaded|62495|
|2022-01|yellow|loaded|2463931|
|2022-02|green|loaded|69399|
|2022-02|yellow|loaded|2979431|
|2022-03|green|loaded|78537|
|2022-03|yellow|loaded|3627882|
|2022-04|green|loaded|76136|
|2022-04|yellow|loaded|3599920|
|2022-05|green|loaded|76891|
|2022-05|yellow|loaded|3588295|
|2022-06|green|loaded|73718|
|2022-06|yellow|loaded|3558124|
|2022-07|green|loaded|64192|
|2022-07|yellow|loaded|3174394|
|2022-08|green|loaded|65929|
|2022-08|yellow|loaded|3152677|
|2022-09|green|loaded|69031|
|2022-09|yellow|loaded|3183767|
|2022-10|green|loaded|69322|
|2022-10|yellow|loaded|3675411|
|2022-11|green|loaded|62313|
|2022-11|yellow|loaded|3252717|
|2022-12|green|loaded|72439|
|2022-12|yellow|loaded|3399549|
|2023-01|green|loaded|68211|
|2023-01|yellow|loaded|3066766|
|2023-02|green|loaded|64809|
|2023-02|yellow|loaded|2913955|
|2023-03|green|loaded|72044|
|2023-03|yellow|loaded|3403766|
|2023-04|green|loaded|65392|
|2023-04|yellow|loaded|3288250|
|2023-05|green|loaded|69174|
|2023-05|yellow|loaded|3513649|
|2023-06|green|loaded|65550|
|2023-06|yellow|loaded|3307234|
|2023-07|green|loaded|61343|
|2023-07|yellow|loaded|2907108|
|2023-08|green|loaded|60649|
|2023-08|yellow|loaded|2824209|
|2023-09|green|loaded|65471|
|2023-09|yellow|loaded|2846722|
|2023-10|green|loaded|66177|
|2023-10|yellow|loaded|3522285|
|2023-11|green|loaded|64025|
|2023-11|yellow|loaded|3339715|
|2023-12|green|loaded|64215|
|2023-12|yellow|loaded|3376567|
|2024-01|green|loaded|56551|
|2024-01|yellow|loaded|2964624|
|2024-02|green|loaded|53577|
|2024-02|yellow|loaded|3007526|
|2024-03|green|loaded|57457|
|2024-03|yellow|loaded|3582628|
|2024-04|green|loaded|56471|
|2024-04|yellow|loaded|3514289|
|2024-05|green|loaded|61003|
|2024-05|yellow|loaded|3723833|
|2024-06|green|loaded|54748|
|2024-06|yellow|loaded|3539193|
|2024-07|green|loaded|51837|
|2024-07|yellow|loaded|3076903|
|2024-08|green|loaded|51771|
|2024-08|yellow|loaded|2979183|
|2024-09|green|loaded|54440|
|2024-09|yellow|loaded|3633030|
|2024-10|green|loaded|56147|
|2024-10|yellow|loaded|3833771|
|2024-11|green|loaded|52222|
|2024-11|yellow|loaded|3646369|
|2024-12|green|loaded|53994|
|2024-12|yellow|loaded|3668371|
|2025-01|green|loaded|48326|
|2025-01|yellow|loaded|3475226|
|2025-02|green|loaded|46621|
|2025-02|yellow|loaded|3577543|
|2025-03|green|loaded|51539|
|2025-03|yellow|loaded|4145257|
|2025-04|green|loaded|52132|
|2025-04|yellow|loaded|3970553|
|2025-05|green|loaded|55399|
|2025-05|yellow|loaded|4591845|
|2025-06|green|loaded|49390|
|2025-06|yellow|loaded|4322960|
|2025-07|green|loaded|48205|
|2025-07|yellow|loaded|3898963|
|2025-08|green|loaded|46306|
|2025-08|yellow|loaded|3574091|
|2025-09|green|loaded|48893|
|2025-09|yellow|loaded|4251015|
|2025-10|green|loaded|49416|
|2025-10|yellow|loaded|4428699|
|2025-11|green|loaded|46912|
|2025-11|yellow|loaded|4181444|
|2025-12|green|**missing**|0|
|2025-12|yellow|**missing**|0|

Se creó una vista a nivel de capa bronce que reporta estos números automáticamente al momento en que se actualicen las entradas en la tabla de datos de los viajes. Por lo pronto, se puede observar que aún no se encuentran disponibles los datos del mes de Diciembre de 2025 de ninguno de los dos servicios de taxis de New York.


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