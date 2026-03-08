if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import urllib.request

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    logger = kwargs.get('logger')
    
    url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

    logger.info(f"Downloading data for Taxi Lookup Zones...")

    try:
        df = pd.read_csv(url, header=0)

        logger.info('Succesfully loaded data for Taxi Lookup Zones.')

        return df

    except urllib.error.HTTPError as e:
            logger.warning(f"File not found or HTTP Error for {file_name}: {e}")
            return pd.DataFrame()


    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
