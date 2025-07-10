import pandas as pd
import requests
from findrum.interfaces import Operator

class SendToAPI(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("SendToAPI expects a DataFrame")

        api_url = self.params.get("url")
        cik_col = self.params.get("id_column", "cik")
        timestamp_col = self.params.get("timestamp_column", "end")
        concept_col = self.params.get("concept_column", "concept")
        value_col = self.params.get("value_column", "val")

        if not api_url:
            raise ValueError("Missing 'url' parameter for API endpoint")

        grouped = input_data.pivot_table(
            index=["accn", cik_col, timestamp_col],
            columns=concept_col,
            values=value_col,
            aggfunc="first"
        ).reset_index()

        grouped.columns.name = None

        for _, row in grouped.iterrows():
            try:
                cik = row[cik_col]
                timestamp = row[timestamp_col]
                accn = row["accn"]

                attributes = {
                    k: v for k, v in row.items()
                    if k not in [cik_col, timestamp_col, "accn"] and pd.notna(v)
                }

                attributes.update({
                    "company_name": input_data["entityName"].unique()[0]
                })

                payload = {
                    "id": str(int(cik)),
                    "type": "financial",
                    "timestamp": str(pd.to_datetime(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")),
                    "source": f"{accn}",
                    "attributes": attributes
                }

                response = requests.post(api_url, json=payload)

                if response.status_code != 200:
                    raise requests.HTTPError(response.text)

            except Exception as e:
                raise RuntimeError(f"Failed to send record {accn} to API: {e}")

        return input_data