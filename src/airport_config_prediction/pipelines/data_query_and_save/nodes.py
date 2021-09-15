"""Nodes for querying and saving data sets.
"""

from kedro.io import DataCatalog, Version
from kedro.extras.datasets.pandas import CSVDataSet
import logging
import pandas as pd


def query_save_version(
    data: pd.DataFrame,
    name: str,
    folder='01_raw',
    versioned=False,
):
    """Saves results of DB query to an @CSV version of the data set

    Note: Assumed that data comes from {name}_data_set@DB and then
    save resulting CSV to data/{folder}/{name}_data_set@CSV
    """
    if versioned:
        version = Version(
            load=None,
            save=None,
        )
    else:
        version = None

    data_set_CSV = CSVDataSet(
        filepath="data/{}/{}_data_set.csv".format(
            folder,
            name.replace(':', '_').replace(' ', '_')
        ),
        save_args={"index": False},
        version=version,
    )
    dc = DataCatalog({"{}_data_set@CSV".format(name): data_set_CSV})

    dc.save("{}_data_set@CSV".format(name), data)


def query_save_version_LAMP(
    data: pd.DataFrame,
    params_globals: dict
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".LAMP",
    )


def query_save_version_METAR(
    data: pd.DataFrame,
    params_globals: dict
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".METAR",
    )


def query_save_version_configs(
    data: pd.DataFrame,
        params_globals: dict
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".configs",
    )


def query_save_version_MFS(
    data: pd.DataFrame,
        params_globals: dict
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".MFS",
    )


def query_save_version_runways(
    data: pd.DataFrame,
        params_globals: dict
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".runways",
    )


def query_save_version_TBFM(
    data: pd.DataFrame,
    params_globals: dict,
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) +".TBFM",
    )


def query_save_version_TFM_track(
    data: pd.DataFrame,
    params_globals: dict,
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) +".TFM_track",
    )


def query_save_version_first_position(
    data: pd.DataFrame,
    params_globals: dict,
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".first_position",
    )


def query_save_version_ETD(
    data: pd.DataFrame,
    params_globals: dict,
):
    query_save_version(
        data,
        params_globals['airport_icao']+'_'+ str(params_globals['start_time'])+ '_'+str(params_globals['end_time']) + ".ETD",
    )
