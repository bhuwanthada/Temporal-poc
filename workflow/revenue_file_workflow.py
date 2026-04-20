from datetime import timedelta
from temporalio import workflow
from fastapi import UploadFile
import io

with workflow.unsafe.imports_passed_through():
    from activity.file_validation_activity import validate_csv_file
    from activity.csv_read_activity import read_csv
    from activity.postgres_lookup_activity import enrich_with_cif_codes, enrich_with_cif_codes_sqlite
    from activity.csv_enrich_activity import write_enriched_csv
    from activity.file_upload_activity import upload_file_to_s3
    from activity.fetch_file_from_s3_bucket import fetch_file_from_s3



@workflow.defn
class RevenueFileWorkflow:

    @workflow.run
    async def run(self, s3_key: str) -> str:
        file_path = await workflow.execute_activity(
            fetch_file_from_s3,
            s3_key,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        await workflow.execute_activity(
            validate_csv_file,
            file_path,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        raw_data = await workflow.execute_activity(
            read_csv,
            file_path,
            schedule_to_close_timeout=timedelta(minutes=1),
        )

        enriched_data = await workflow.execute_activity(
            enrich_with_cif_codes_sqlite,
            raw_data,
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        enriched_file_path = await workflow.execute_activity(
            write_enriched_csv,
            {"file_path": file_path, "enriched_data": enriched_data},
            schedule_to_close_timeout=timedelta(minutes=1),
        )

        await workflow.execute_activity(
            upload_file_to_s3,
            {
                "original_file_path": file_path,
                "enriched_file_path": enriched_file_path,
                "s3_key":s3_key
            },
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        return "Revenue file processed successfully"
