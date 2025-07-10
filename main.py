from findrum import Platform

if __name__ == "__main__":
    platform = Platform("config/config.yaml",verbose=True)
    platform.register_pipeline("pipelines/silver_to_gold_llm.yaml")
    platform.register_pipeline("pipelines/silver_to_gold_subject_store.yaml")
    platform.register_pipeline("pipelines/bronze_to_silver_pipeline.yaml")
    platform.register_pipeline("pipelines/ingestion_pipeline.yaml")
    platform.start()