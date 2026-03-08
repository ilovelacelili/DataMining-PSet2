if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import urllib.request
from datetime import datetime
import pytz

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    logger = kwargs.get('logger')
    
    # Fetch parameters backfill triggers
    # Defaults to Yellow Taxi, January 2024 if not provided
    execution_date = kwargs.get('execution_date')
    if execution_date:
        year = execution_date.year
        month = execution_date.month
    else:
        year = 2024
        month = 1

    source_month = f"{year}-{month:02d}"
    dataframes = []

    # SCHEMA DRIFT PROTECTION ---
    # Define exactly what columns we care about for this project 
    # for when there are new columns that do not fit our schema
    target_columns = [
        'VendorID', 
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', # Yellow
        'lpep_pickup_datetime', 'lpep_dropoff_datetime', # Green
        'passenger_count', 'trip_distance',
        'PULocationID', 'DOLocationID',
        'RatecodeID', 'payment_type',
        'fare_amount', 'tip_amount', 'total_amount',
        
        # Our metadata
        'ingest_ts', 'source_month', 'service_type'
    ]

    for service_type in ['yellow', 'green']:
    
        file_name = f"{service_type}_tripdata_{source_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    
        logger.info(f"Downloading {service_type} data for {source_month}...")
    
        try:
            df_temp = pd.read_parquet(url, engine='pyarrow')

            # Add Mandatory Ingestion Metadata
            # ingest_ts (timestamp), source_month (YYYY-MM), service_type
            df_temp['ingest_ts'] = datetime.now(pytz.UTC)
            df_temp['source_month'] = source_month 
            df_temp['service_type'] = service_type

            # This prevents KeyErrors (e.g., trying to select 'tpep' from a Green taxi file)
            # Keep only columns that are IN the target list AND ACTUALLY EXIST in this specific df
            columns_to_keep = [col for col in target_columns if col in df_temp.columns]
            df_temp = df_temp[columns_to_keep]

            dataframes.append(df_temp)

            logger.info(f"Successfully loaded {len(df_temp)} rows for {service_type}.")
        except urllib.error.HTTPError as e:
            logger.warning(f"File not found or HTTP Error for {file_name}: {e}")
            return pd.DataFrame() # Return empty DF if month doesn't exist yet
        
    
    if not dataframes:
        logger.warning(f"No data found for {source_month}.")
        return pd.DataFrame()
    

    return pd.concat(dataframes, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
