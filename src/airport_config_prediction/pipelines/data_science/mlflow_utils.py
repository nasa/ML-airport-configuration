from typing import Dict, Any
import mlflow


def init_mlflow(
    parameters: Dict[str, Any]
) -> None:

    mlflow.set_tracking_uri(parameters['mlflow']['tracking_uri'])
    mlflow.set_experiment(parameters['mlflow']['experiment_name'])
    active_run = mlflow.start_run(run_name=parameters['mlflow']['run_name'])
    mlflow.set_tag('modeler_name', parameters['mlflow']['modeler_name'])
    mlflow.set_tag('airport_icao', parameters['globals']['airport_icao']) # to add, after setting single airport run
    mlflow.log_param("features", list(parameters['inputs'].keys()))
    core_features=[n for n in parameters["inputs"]
                   if parameters["inputs"][n]["core"] == True]
    mlflow.log_param("core_features", core_features)

    features=[f for f in parameters["inputs"]]
    mlflow.log_param("features", features)

    for key, value in parameters.items():
        mlflow.log_param(key, value)

    return active_run
