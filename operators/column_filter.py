import pandas as pd
from findrum.interfaces import Operator

class ColumnFilter(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("ColumnFilter expects a DataFrame")

        include = self.params.get("include")
        exclude = self.params.get("exclude")

        if include:
            input_data = input_data[include]

        if exclude:
            input_data = input_data.drop(columns=exclude, errors="ignore")

        return input_data