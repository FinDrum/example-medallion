import pandas as pd
import re
from findrum.interfaces import Operator

class ValueFilter(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("ValueFilter expects a DataFrame")

        df = input_data.copy()
        filters = self.params.get("filters", [])

        if not filters:
            return df

        mask = pd.Series(True, index=df.index)

        for f in filters:
            col = f["column"]
            col_mask = pd.Series(True, index=df.index)

            allowed = f.get("allowed_values")
            regex   = f.get("regex")
            include_na = f.get("include_na", True)

            if allowed is not None:
                col_mask &= df[col].isin(allowed)

            if regex is not None:
                col_mask &= df[col].astype(str).str.match(regex, na=False)

            if not include_na:
                col_mask &= df[col].notna()

            mask &= col_mask

        return df[mask].copy()
