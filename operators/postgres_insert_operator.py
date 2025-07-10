import pandas as pd
from sqlalchemy import create_engine, text
from findrum.interfaces import Operator

class PostgresInsertOperator(Operator):
    def run(self, input_data: pd.DataFrame):
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("PostgresInsertOperator expects a pandas DataFrame")

        db_url = self.params["db_url"]
        table_name = self.params["table"]
        if_exists = self.params.get("if_exists", "append")  

        engine = create_engine(db_url)

        try:
            input_data.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                method="multi"
            )
        except Exception as e:
            raise

        engine.dispose()
        return input_data
