# Copyright 2020 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application entry point."""
from pathlib import Path
from typing import Any, Dict, Iterable
import yaml
import datetime
import os, shutil

#from src.airport_config_prediction.pipeline import create_pipelines
from kedro.framework.context import KedroContext, load_package_context
from kedro.pipeline import Pipeline
from kedro.runner import AbstractRunner
from src.airport_config_prediction.pipeline import create_pipelines
from kedro.config import TemplatedConfigLoader  # new import
class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """

    project_name = "Airport Runway Configuration Prediction"
    # `project_version` is the version of kedro used to generate the project
    project_version = "0.16.1"
    package_name = "airport_config_prediction"
    globals_batch_mode = False # when True, a copy of globals.yml, with "_batch" in name, is used for batch processing

    def _get_globals_pattern(self):
        try:
            if self._extra_params["global_config_yml"] is not None:
                globals_pattern = self._extra_params["global_config_yml"]
        except (TypeError, KeyError):
            # Default is to read the globals dictionary from
            # project base config directory globals.yml
            globals_pattern = "globals.yml"

        if self.globals_batch_mode:
            globals_pattern = '_batch.'.join(globals_pattern.split('.'))

        return globals_pattern

    def _create_config_loader(self, conf_paths) -> TemplatedConfigLoader:

        globals_pattern = self._get_globals_pattern()

        # Not exactly sure why one would use this dict
        globals_dict = {}

        return TemplatedConfigLoader(
            conf_paths,
            globals_pattern=globals_pattern,
            globals_dict=globals_dict,
        )

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines()

    def run(
        self,
        tags: Iterable[str] = None,
        runner: AbstractRunner = None,
        node_names: Iterable[str] = None,
        from_nodes: Iterable[str] = None,
        to_nodes: Iterable[str] = None,
        from_inputs: Iterable[str] = None,
        load_versions: Dict[str, str] = None,
        pipeline_name: str = None,
    ) -> Dict[str, Any]:

        globals_values = self.config_loader._arg_dict

        if ('batch_mode' in globals_values['globals']) and \
                (pipeline_name in globals_values['globals']['batch_mode']['pipelines']):
            globals_file = self._get_globals_pattern()
            self.globals_batch_mode = True
            globals_batch_file = self._get_globals_pattern()

            # Create temporary globals batch file, used in batch mode
            shutil.copy('./conf/base/' + globals_file, './conf/base/' + globals_batch_file)

            start_time = globals_values['globals']['start_time']
            end_time = globals_values['globals']['end_time']
            batch_days = globals_values['globals']['batch_mode']['batch_days']
            overlap_days = globals_values['globals']['batch_mode']['overlap_days']

            # Store run start/end time
            globals_values['globals']['batch_mode']['run_start_time'] = start_time
            globals_values['globals']['batch_mode']['run_end_time'] = end_time

            start_time_b = start_time
            end_time_b = start_time + datetime.timedelta(days=batch_days)
            while start_time_b < end_time:

                if end_time_b > end_time:
                    end_time_b = end_time

                globals_values['globals']['start_time'] = start_time_b
                # overlap_days added to ensure features capturing future information are correctly calculated at the
                # end of the batch time period
                globals_values['globals']['end_time'] = end_time_b + datetime.timedelta(days=overlap_days)

                # Update globals and catalog
                with open('./conf/base/'+globals_batch_file, 'w') as file:
                    yaml.dump(globals_values, file)

                self._get_catalog();

                # run pipeline
                KedroContext.run(self,
                    tags=tags,
                    runner=runner,
                    node_names=node_names,
                    from_nodes=from_nodes,
                    to_nodes=to_nodes,
                    from_inputs=from_inputs,
                    load_versions=load_versions,
                    pipeline_name=pipeline_name,
                )

                # Update for next batch
                start_time_b = start_time_b + datetime.timedelta(days=batch_days)
                end_time_b = end_time_b + datetime.timedelta(days=batch_days)

            # Remove temporary batch globals file
            os.remove('./conf/base/'+globals_batch_file)

        else:
            # Default Kedro run method
            KedroContext.run(self,
                             tags=tags,
                             runner=runner,
                             node_names=node_names,
                             from_nodes=from_nodes,
                             to_nodes=to_nodes,
                             from_inputs=from_inputs,
                             load_versions=load_versions,
                             pipeline_name=pipeline_name,
                             )


def run_package():
    # Entry point for running a Kedro project packaged with `kedro package`
    # using `python -m <project_package>.run` command.
    project_context = load_package_context(
        project_path=Path.cwd(),
        package_name=Path(__file__).resolve().parent.name
    )
    project_context.run()


if __name__ == "__main__":
    run_package()
