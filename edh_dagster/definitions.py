from dagster import Definitions

from edh_dagster.assets import all_assets
from edh_dagster.checks import all_checks
from edh_dagster.schedules import all_schedules

defs = Definitions(assets=all_assets, asset_checks=all_checks, schedules=all_schedules)
