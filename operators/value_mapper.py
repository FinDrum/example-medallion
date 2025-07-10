import pandas as pd
import re
from findrum.interfaces import Operator

class ValueMapper(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("ValueMapper expects a DataFrame")

        df = input_data.copy()
        mappings = self.params.get("mappings", [])

        if not mappings:
            return df

        for m in mappings:
            input_col = m["column"]
            output_col = m.get("output_column", input_col)

            if "regex" in m:
                pattern = m["regex"]
                replacement = m.get("regex_replacement", "")
                df[output_col] = df[input_col].astype(str).apply(
                    lambda x: re.sub(pattern, replacement, x)
                )
            elif "mapping" in m:
                flat_mapping = {
                    original: alias
                    for alias, originals in m["mapping"].items()
                    for original in (originals if isinstance(originals, list) else [originals])
                }
                default = m.get("default", pd.NA)
                mapped = df[input_col].map(flat_mapping).fillna(default)
                df[output_col] = mapped.apply(lambda x: x[0] if isinstance(x, list) else x)
            else:
                raise ValueError(f"Missing 'mapping' or 'regex' in mapping config: {m}")

        return df.dropna()
