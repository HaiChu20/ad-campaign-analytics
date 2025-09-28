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