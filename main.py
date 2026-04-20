import logging
import os  
from fastapi import FastAPI, UploadFile, File, HTTPException
from utils.csv_utils import validate_csv_columns
from utils.s3_utils import upload_bytes_to_s3
from utils.time_utils import ist_hour_prefix
from utils.file_utils import safe_filename, read_uploadfile_bytes
from workflow.revenue_file_workflow import RevenueFileWorkflow
from workflow.hold_account_amount_workflow import HoldAccountWithPenaltyWorkflow
from dotenv import load_dotenv
from temporalio.client import Client

logger = logging.getLogger(__name__)
load_dotenv()

SUPPORTED_ACTIONS = {
    "extracting_users": RevenueFileWorkflow.run,
    "hold_account_with_penality_amount": HoldAccountWithPenaltyWorkflow.run,
}

S3_BUCKET = os.getenv("S3_BUCKET")

app = FastAPI(title="Garnishi Workflow", version="1.0.0")

temporal_client = None

temporal_host_path = os.getenv("TEMPORAL_HOST_PATH", "localhost:7233")


@app.get("/health")
async def health_check():
    return {"status": "up", "message": "System is up and running"}

@app.on_event("startup")
async def startup_event():
    global temporal_client
    # temporal_client = await Client.connect(temporal_host_path, "localhost:7233")
    temporal_client = await Client.connect(
    temporal_host_path,
    namespace="cba-temporal.pz8tx",
    api_key=os.getenv("TEMPORAL_API_KEY"),
    tls=True,
)
    if not temporal_client:
        raise Exception("Temporal client is not turning up.")
    else:
        logger.info("Temporal client is up.")


@app.post("/load-revenue-file")
async def load_revenue_file(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name is missing.")

        filename = safe_filename(file.filename)

        if not filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only .csv files are allowed.")

        # 2) Validate required columns
        content_bytes = await read_uploadfile_bytes(file)
        validate_csv_columns(content_bytes)

        hour_prefix = ist_hour_prefix()
        s3_key = f"{hour_prefix}/telemetry/{filename}"
        upload_bytes_to_s3(
            bucket=S3_BUCKET,
            key=s3_key,
            data=content_bytes,
            content_type="text/csv",
        )
        workflow_run_method = SUPPORTED_ACTIONS["extracting_users"]
        workflow_run = await temporal_client.execute_workflow(
            workflow_run_method,
            s3_key,
            id=f"extracting_users-{file.filename.replace('/', '-')}",
            task_queue="revenue-file-queue",
        )
        new_s3_key = s3_key.replace("telemetry", "cif-codes")
        return {
        "message": "File processed successfully. Processed file will be available in S3 after workflow completion.",
        "key": new_s3_key,
               }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")


@app.post("/put-amount-on-hold")
async def put_amount_on_hold(file: UploadFile = File(...)):
        try:
            if not file.filename:
                raise HTTPException(status_code=400, detail="File name is missing.")

            filename = safe_filename(file.filename)

            if not filename.lower().endswith(".csv"):
                raise HTTPException(status_code=400, detail="Only .csv files are allowed.")

            # 2) Validate required columns
            content_bytes = await read_uploadfile_bytes(file)
            validate_csv_columns(content_bytes, telemetery_amount_put_on_hold=True)

            hour_prefix = ist_hour_prefix()
            s3_key = f"{hour_prefix}/telemetry-amount-hold/{filename}"
            upload_bytes_to_s3(
                bucket=S3_BUCKET,
                key=s3_key,
                data=content_bytes,
                content_type="text/csv",
            )
            workflow_run_method = SUPPORTED_ACTIONS["hold_account_with_penality_amount"]
            workflow_run = await temporal_client.execute_workflow(
                workflow_run_method,
                s3_key,
                id=f"extracting_users-{file.filename.replace('/', '-')}",
                task_queue="revenue-file-queue",
            )
            new_s3_key = s3_key.replace("telemetry-amount-hold", "freezed-amount-on-account")
            return {
            "message": "File processed successfully. Processed file will be available in S3 after workflow completion.",
            "key": new_s3_key,
                }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error while processing: {str(e)}")

