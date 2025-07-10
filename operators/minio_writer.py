import pandas as pd
import io
import boto3
from findrum.interfaces import Operator

class MinioWriter(Operator):
    def run(self, input_data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(input_data, pd.DataFrame):
            raise ValueError("MinioWriter expects a DataFrame")
        
        bucket = self.params.get("bucket")
        prefix = self.params.get("prefix", "").rstrip("/")
        aws_access_key = self.params.get("access_key")
        aws_secret_key = self.params.get("secret_key")
        endpoint_url = self.params.get("endpoint_url")
        file_format = self.params.get("format", "csv").lower()

        if not all([bucket, aws_access_key, aws_secret_key, endpoint_url]):
            raise ValueError("Missing required parameters for MinIO write")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            endpoint_url=endpoint_url,
        )

        for cik, group_df in input_data.groupby("cik"):
            if file_format == "csv":
                buffer = io.StringIO()
                group_df.to_csv(buffer, index=False)
                content = buffer.getvalue()
            elif file_format == "json":
                buffer = io.StringIO()
                group_df.to_json(buffer, orient="records", lines=True)
                content = buffer.getvalue()
            elif file_format == "parquet":
                buffer = io.BytesIO()
                group_df.to_parquet(buffer, index=False, engine="pyarrow")
                content = buffer.getvalue()
            else:
                raise ValueError(f"Unsupported format: {file_format}")

            key = f"{prefix}/{cik}.{file_format}"
            s3.put_object(Bucket=bucket, Key=key, Body=content)

        return input_data