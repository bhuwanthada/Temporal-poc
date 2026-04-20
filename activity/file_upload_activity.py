import boto3
import logging
import os
import shutil
from temporalio import activity
from typing import Dict
from exceptions import FileUploadError

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

@activity.defn
def upload_file_to_s3(data: Dict) -> None:
    original = data.get("original_file_path")
    enriched = data.get("enriched_file_path")
    s3_key = data.get("s3_key")
    put_amount_on_hold_workflow = data.get("put_amount_on_hold_workflow", None)
    
    if s3_key and put_amount_on_hold_workflow:
        new_s3_key = s3_key.replace("telemetry-amount-hold", "freezed-amount-on-account")
    else:
        new_s3_key = s3_key.replace("telemetry", "cif-codes")

    try:
        s3 = boto3.client("s3")
        s3.upload_file(
            Bucket=S3_BUCKET,
            Key=new_s3_key,
            Filename=str(enriched),
            ExtraArgs={"ContentType": "text/csv"}
        )
        logger.info("File uploaded to S3 bucket successfully.")
    except Exception as exc:
        logger.exception("File upload failed")
        raise FileUploadError(str(exc)) from exc
