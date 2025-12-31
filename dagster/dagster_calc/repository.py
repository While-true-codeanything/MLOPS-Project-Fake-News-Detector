from dagster import Definitions
from .assets import minute_stats
from .jobs import minute_stats_job
from .schedules import minute_stats_schedule

defs = Definitions(
    assets=[minute_stats],
    jobs=[minute_stats_job],
    schedules=[minute_stats_schedule],
)
