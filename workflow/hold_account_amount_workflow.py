from datetime import timedelta
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activity.file_validation_activity import validate_csv_file
    from activity.read_masked_cif_csv_activity import process_masked_cif_data, process_masked_cif_data_sqlite
    from activity.hold_amount_activity import hold_account_amount, hold_account_amount_sqlite
    from activity.write_amount_hold_csv_activity import write_amount_hold_csv
    from activity.fetch_file_from_s3_bucket import fetch_file_from_s3
    from activity.file_validation_activity import validate_csv_file
    from activity.csv_read_activity import read_amount_on_hold_csv
    from activity.csv_enrich_activity import write_enriched_csv
    from activity.file_upload_activity import upload_file_to_s3


@workflow.defn
class HoldAccountWithPenaltyWorkflow:

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
            read_amount_on_hold_csv,
            file_path,
            schedule_to_close_timeout=timedelta(minutes=1),
        )

        proessed_user_data = await workflow.execute_activity(
            process_masked_cif_data_sqlite,
            raw_data,
            schedule_to_close_timeout=timedelta(minutes=5),
        )

        processed_file_path = await workflow.execute_activity(
            write_enriched_csv,
            {"file_path": file_path, "enriched_data": proessed_user_data},
            schedule_to_close_timeout=timedelta(minutes=1),
        )
        
        await workflow.execute_activity(
            upload_file_to_s3,
            {
                "original_file_path": file_path,
                "enriched_file_path": processed_file_path,
                "s3_key":s3_key,
                "put_amount_on_hold_workflow": True
            },
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        return "Hold operation completed"
