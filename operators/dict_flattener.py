import pandas as pd
from findrum.interfaces import Operator

class DictFlattener(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:

        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        if input_data.empty:
            raise ValueError("Input DataFrame is empty")
        
        dict_column = self.params.get("dict_column", "data")
        keep_columns = self.params.get("keep_columns", [])
        level_keys = self.params.get("level_keys", [])

        try:
            row = input_data.iloc[0]
            root = row[dict_column]
        except Exception as e:
            raise ValueError(f"Could not extract root dictionary: {e}")

        if not isinstance(root, dict):
            raise ValueError(f"Expected dict in column '{dict_column}', got {type(root)}")

        flattened = []

        def recurse(node, level_path):
            if isinstance(node, list):
                for entry in node:
                    if not isinstance(entry, dict):
                        continue

                    record = {col: row[col] for col in keep_columns if col in row}

                    for i, key in enumerate(level_path):
                        if i < len(level_keys):
                            record[level_keys[i]] = key
                        else:
                            record[f"level_{i}"] = key

                    record.update(entry.copy())
                    flattened.append(record)
                return

            if isinstance(node, dict):
                for key, child in node.items():
                    recurse(child, level_path + [key])

        recurse(root, [])

        return pd.DataFrame(flattened)