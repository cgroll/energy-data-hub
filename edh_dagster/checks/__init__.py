from edh_dagster.checks.gas import gas_checks
from edh_dagster.checks.smard import hourly_gap_checks

all_checks = [*hourly_gap_checks, *gas_checks]
