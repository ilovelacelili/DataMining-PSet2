from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    logger = kwargs.get('logger')
    
    if df.empty:
        logger.warning("No data to export. Skipping.")
        return

    # Extract partition identifiers from the dataframe
    source_month = df['source_month'].iloc[0]

    schema_name = 'raw'
    table_name = 'raw_taxi_trips'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        # SAFE CHECK: Does the table exist?
        check_table_query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
            );
        """
        
        # loader.load() returns a Pandas DataFrame. We extract the boolean result.
        table_exists_df = loader.load(check_table_query)
        table_exists = table_exists_df.iloc[0, 0]

        if table_exists:
            # IDEMPOTENCY: Delete existing data for this month/service
            # This guarantees that re-running the pipeline won't duplicate rows.
            delete_query = f"""
                DELETE FROM {schema_name}.{table_name} 
                WHERE source_month = '{source_month}' 
                AND service_type IN ('yellow', 'green');
            """
            loader.execute(delete_query)
            logger.info(f"Idempotency check: Deleted old data for {source_month}")
        else:
            logger.info(f"Table does not exist yet. Skipping DELETE and Proceeding to create/insert.")
        

        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='append',
            chunksize=10000 # Prevents memory crashes on massive datasets
        )
    logger.info("Export completed successfully.")
