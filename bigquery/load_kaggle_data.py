import os
import pandas as pd
import zipfile
import kagglehub
import yaml
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi
from kagglehub import KaggleDatasetAdapter

class KaggleDataLoader:
    def __init__(self):
        """Initialize clients using configuration"""
        # Load configuration from dbt profiles.yml
        self.project_id = self.get_project_id_from_dbt_profiles()
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # Set up Kaggle API with credentials
        self.kaggle_api = KaggleApi()
        self.kaggle_api.authenticate()
    
    def get_project_id_from_dbt_profiles(self):
        with open('/opt/dbt/profiles.yml', 'r') as f:
            profiles = yaml.safe_load(f)
        return profiles['ad_campaign_project']['outputs']['dev']['project']
    
    def download_and_load_dataset(self, dataset_name, file_name, table_name): 

            # Load the latest version
            df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            dataset_name,
            file_name
            )

            self.load_dataframe_to_bigquery(df, table_name)
    
    def load_dataframe_to_bigquery(self, df, table_name):
        table_id = f"{self.project_id}.ad_campaign_raw.{table_name}"
        
        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Append to existing data
            autodetect=True,  # Auto-detect schema
        )
        
        # Load DataFrame to BigQuery
        job = self.bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion

def main():
    loader = KaggleDataLoader()
    
    # Simple list of datasets to load
    datasets = [
        ('madislemsalu/facebook-ad-campaign', 'data.csv', 'campaigns'),
    ]
    
    for dataset_name, file_name, table_name in datasets:
        try:
            loader.download_and_load_dataset(dataset_name, file_name, table_name)
        except Exception as e:
            print(f"⚠️  Skipping {dataset_name}: {e}")

if __name__ == "__main__":
    main()