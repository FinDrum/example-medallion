from findrum.interfaces import Operator
import pandas as pd
import boto3
import io
import json
import logging

logger = logging.getLogger("findrum")

class MinioReader(Operator):
    def run(self, input_data=None) -> pd.DataFrame:
        bucket = self.params.get("bucket") or (input_data.get("bucket") if input_data else None)
        prefix = self.params.get("prefix") or (input_data.get("file_path") if input_data else None)
        endpoint_url = self.params.get("endpoint_url", "http://localhost:9000")
        access_key = self.params.get("access_key", "minioadmin")
        secret_key = self.params.get("secret_key", "minioadmin")

        if not bucket or not prefix:
            raise ValueError("Missing required parameters: 'bucket' and 'prefix'")

        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        logger.info(f"📂 Reading from MinIO bucket='{bucket}', prefix='{prefix}'")
        objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in objects:
            logger.info("⚠️ No data found for this prefix.")
            return pd.DataFrame()

        all_data = []

        for obj in objects.get("Contents", []):
            key = obj["Key"]
            file_format = None

            if key.endswith(".json"):
                file_format = "json"
            elif key.endswith(".csv"):
                file_format = "csv"
            elif key.endswith(".parquet"):
                file_format = "parquet"
            else:
                logger.info(f"⏭ Ignored file (unsupported format): {key}")
                continue

            try:
                response = client.get_object(Bucket=bucket, Key=key)
                content = response["Body"].read()

                if file_format == "json":
                    data = json.loads(content)
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    else:
                        df = pd.DataFrame([data])
                elif file_format == "csv":
                    df = pd.read_csv(io.StringIO(content.decode()))
                elif file_format == "parquet":
                    df = pd.read_parquet(io.BytesIO(content))

                all_data.append(df)

            except Exception as e:
                logger.warning(f"❌ Error reading {key}: {e}")

        if not all_data:
            logger.info("⚠️ No supported data files loaded.")
            return pd.DataFrame()

        final_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"✅ Loaded {len(final_df)} records from MinIO.")
        return final_df
