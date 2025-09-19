import logging
import os
import io
import csv
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import requests
import datetime
from utils import create_email_csv

START_DATE = datetime.date(2025, 6, 1)
EMAIL = "r3finance@nsbe.org"
STORAGE_ACCOUNT_NAME = "projectworkflowae1e"
CONTAINER_NAME = "llm-example-dataset"
BLOB_NAME = "jurmain_emails.csv"
# Get storage account name from settings
account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
credential = DefaultAzureCredential()
blob_service_client = BlobServiceClient(account_url, credential=credential)

container_name = CONTAINER_NAME
blob_name = BLOB_NAME

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing email update function")

    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Call your helper to get DataFrame
    df_csv = create_email_csv(START_DATE, EMAIL)

    # Convert DataFrame to CSV (in memory, not saved locally)
    output = io.StringIO()
    df_csv.to_csv(output, index=False)  # no index column
    csv_data = output.getvalue()

    # Upload to blob (overwrite if exists)
    blob_client.upload_blob(csv_data, overwrite=True)

    return func.HttpResponse(
        "emails.csv uploaded to blob storage",
        status_code=200
    )

    

  

