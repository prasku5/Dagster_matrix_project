# schedules/weekly_schedule.py

from dagster import ScheduleDefinition

weekly_schedule = ScheduleDefinition(
    job_name="weekly_job",
    cron_schedule="0 14 * * 0",  # Runs weekly on Sundays at 2:00 PM
    execution_timezone="UTC"
)
