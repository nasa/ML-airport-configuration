"""Construction of the master pipeline.
"""

from typing import Dict
from kedro.pipeline import Pipeline

from airport_config_prediction.pipelines import data_query_and_save as dqs
from airport_config_prediction.pipelines import data_engineering as de


from airport_config_prediction.pipelines import data_science as ds
#from .pipelines import test as airport_test

from data_services.conda_environment_test import check_environment


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """

    check_environment()

    dqs_pipelines = dqs.create_pipelines()
    data_engineering_pipeline=de.create_airport_config_de_pipeline()



    #pipelines = {}
    #for airport_icao, dqs_pipeline in dqs_pipelines.items():
    #   pipelines['dqs_' + airport_icao] = dqs_pipeline

    #ipelines['__default__']=pipelines['de_all']

    ds_pipeline = ds.create_pipelines( )


    return {

        "airport_config_dqs": dqs_pipelines,
        "airport_config_de": data_engineering_pipeline,
        "airport_config_ds": ds_pipeline['airport_config'],
        "airport_config_full": data_engineering_pipeline + ds_pipeline['airport_config'],
        "de": data_engineering_pipeline,


    }
