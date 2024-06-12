# schedules/daily_schedule.py

from dagster import ScheduleDefinition

daily_schedule = ScheduleDefinition(
    job_name="my_job",
    cron_schedule="0 0 * * *",  # Runs daily at 12:05 AM
    execution_timezone="UTC"
)

