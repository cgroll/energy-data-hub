"""Scheduling for the hub's own assets.

This is the actual gap DVC couldn't fill: a daily pull of whatever's new
upstream, independent of any book repo choosing to consume it yet. Books
stay decoupled (see the `book_*` external assets defined in their own
repos) -- refreshing here never touches a book's own build.
"""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

refresh_smard_job = define_asset_job(
    name="refresh_smard_job",
    selection=AssetSelection.groups(
        "smard_generation",
        "smard_consumption",
        "smard_price",
        "smard_forecast",
        "smard_capacity",
        "redispatch",
    ),
)

# 06:00 Europe/Berlin daily. SMARD/netztransparenz publish with a lag of a
# few hours to a few days depending on series, so a morning pull is early
# enough to matter without hammering the endpoints.
refresh_smard_daily = ScheduleDefinition(
    name="refresh_smard_daily",
    job=refresh_smard_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Berlin",
)

all_schedules = [refresh_smard_daily]
