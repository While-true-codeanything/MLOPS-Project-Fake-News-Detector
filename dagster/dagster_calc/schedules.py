from dagster import ScheduleDefinition, DefaultScheduleStatus
from .jobs import minute_stats_job

minute_stats_schedule = ScheduleDefinition(
    job=minute_stats_job,
    cron_schedule="*/1 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)
