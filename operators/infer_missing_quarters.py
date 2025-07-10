import pandas as pd
import numpy as np
from findrum.interfaces import Operator

class InferMissingQuarters(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        start_col = self.params['start_date_column']
        end_col = self.params['end_date_column']

        df = input_data.copy()
        df[start_col] = pd.to_datetime(df[start_col])
        df[end_col] = pd.to_datetime(df[end_col])

        df = self.__get_time_frame(df)
        df = df.rename(columns={start_col: "start", end_col: "end"})

        new_rows = []

        for concept, concept_df in df.groupby("concept"):
            annuals = concept_df[concept_df["frame"] == "Y"]

            for _, annual in annuals.iterrows():
                fiscal_start = annual["start"]
                fiscal_end = annual["end"]
                total_val = annual["val"]

                in_range = concept_df[
                    (concept_df["frame"].str.contains("Q")) &
                    (concept_df["start"] >= fiscal_start) &
                    (concept_df["end"] <= fiscal_end)
                ]

                present_quarters = set(in_range["frame"].dropna())
                expected_quarters = {"Q1", "Q2", "Q3", "Q4"}
                missing_quarters = expected_quarters - present_quarters

                if len(present_quarters) < 3 or not missing_quarters:
                    continue

                known_val = in_range["val"].sum()
                residual_val = total_val - known_val
                share_val = residual_val / len(missing_quarters)

                for q in sorted(missing_quarters):
                    q_num = int(q[1])
                    
                    in_range = concept_df[
                        (concept_df["frame"].str.contains("Q")) &
                        (concept_df["start"].dt.year == fiscal_end.year) &
                        (concept_df["end"].dt.year == fiscal_end.year)
                    ]

                    prev_q = f"Q{q_num - 1}"
                    next_q = f"Q{q_num + 1}"

                    prev_row = in_range[in_range["frame"] == prev_q]
                    next_row = in_range[in_range["frame"] == next_q]

                    if not prev_row.empty:
                        q_start = prev_row.iloc[0]["end"] + pd.Timedelta(days=1)
                    else:
                        q_start = fiscal_start

                    if not next_row.empty:
                        q_end = next_row.iloc[0]["start"] - pd.Timedelta(days=1)
                    else:
                        q_end = fiscal_end

                    fiscal_year = q_end.year

                    new_rows.append({
                        "cik": annual.get("cik", np.nan),
                        "entityName": annual.get("entityName", np.nan),
                        "concept": concept,
                        "val": share_val,
                        "start": q_start,
                        "end": q_end,
                        "year": fiscal_year,
                        "frame": q,
                    })

        if new_rows:
            df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

        df = df.sort_values(["concept", "start"]).reset_index(drop=True)

        return df

    
    def __get_time_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["year"] = df["end"].dt.year
        df["frame"] = self.__get_quarter(df)  
        return df

    def __get_quarter(self, df: pd.DataFrame) -> pd.Series:
        period_days = (df["end"] - df["start"]).dt.days + 1
        is_annual = period_days > 275

        month = df["end"].dt.month

        fiscal_month = (month - 3) % 12 + 1

        quarter_num = pd.cut(
            fiscal_month,
            bins=[0, 3, 6, 9, 12],
            labels=["Q1", "Q2", "Q3", "Q4"],
            right=True
        )

        frame = np.where(is_annual, "Y", quarter_num.astype(str))
        return pd.Series(frame, index=df.index)