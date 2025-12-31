from dagster import define_asset_job

minute_stats_job = define_asset_job(
    name="stats_minute_job",
    selection=["stats_minute"],
)