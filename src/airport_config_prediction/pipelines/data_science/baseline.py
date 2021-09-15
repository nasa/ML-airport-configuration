from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class PropagateCurrentConfigModel(BaseEstimator, TransformerMixin):

    def __init__(
            self,
            LATs: list
    ) -> None:
        self.LATs = LATs

    def predict(
            self,
            data: pd.DataFrame,
    ) -> np.array:
        predicted = []

        for v in data['airport_configuration_name_current']:
            predicted.append({'value': np.array([v] * len(self.LATs)),
                              'lookahead': self.LATs})

        return predicted