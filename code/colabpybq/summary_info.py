# initialize variables
problematic_job_id = <JOB-ID>
admin_project_id = <PROJECT-ID>
# initialize bigquery client
client = bigquery.Client(project=admin_project_id)

# query organization system tables
job_execution_sql = f'''
 SELECT
   project_id,
   job_id,
   reservation_id,
   EXTRACT(DATE FROM creation_time) AS creation_date,
   creation_time,
   end_time,
   TIMESTAMP_DIFF(end_time, start_time, SECOND) AS job_duration_seconds,
   job_type,
   user_email,
   state,
   error_result,
   total_bytes_processed,
   -- Average slot utilization per job is calculated by dividing
   -- total_slot_ms by the millisecond duration of the job
   SAFE_DIVIDE(total_slot_ms, (TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))) AS avg_slots
 FROM
   `region-US`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
 ORDER BY
   creation_time DESC
'''

# save results as a dataframe
jobs_by_org = client.query(job_execution_sql).to_dataframe()
# display summary of job information
jobs_by_org.loc[jobs_by_org['job_id'] == problematic_job_id]
