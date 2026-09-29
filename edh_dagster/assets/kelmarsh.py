"""Kelmarsh wind farm data (Zenodo record 5841834) as Dagster
software-defined assets. See `edh/kelmarsh.py` for source, provenance, and
the `full_refresh` reasoning.
"""

from dagster import AssetExecutionContext, MetadataValue, asset

from edh.kelmarsh import download_grid_meter, download_turbine_scada, download_wt_static
from edh.paths import kelmarsh_grid_meter_file, kelmarsh_turbine_scada_file, kelmarsh_wt_static_file

_SOURCE_URL = "https://zenodo.org/records/5841834"
_SOURCE = "Kelmarsh wind farm data (Cubico Sustainable Investments Ltd, Zenodo record 5841834, CC-BY-4.0)"


@asset(
    group_name="kelmarsh",
    kinds={"api", "parquet"},
    tags={"load_pattern": "full_refresh"},
    description=(
        "Per-turbine static specs (coordinates, rated power, hub height, rotor diameter, commercial "
        "operations date) for Kelmarsh wind farm's 6 Senvion MM92 units (12.3 MW total)."
    ),
    metadata={
        "source": _SOURCE,
        "source_url": MetadataValue.url(_SOURCE_URL),
        "region": "UK (Kelmarsh, Northamptonshire)",
        "resolution": "static -- one row per turbine, no time dimension",
        "unit": "kW (rated power), m (hub height / rotor diameter / elevation), degrees (lat/lon)",
        "timestamp_timezone": "n/a -- Commercial Operations Date is a bare calendar date",
        "update_pattern": "full_refresh: re-downloads and overwrites the whole (small, static) file every run",
    },
)
def kelmarsh_wt_static(context: AssetExecutionContext) -> None:
    df = download_wt_static()
    output_file = kelmarsh_wt_static_file()
    df.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(df.to_markdown(index=False)),
        }
    )


@asset(
    group_name="kelmarsh",
    kinds={"api", "parquet"},
    tags={"load_pattern": "full_refresh"},
    description=(
        "10-minute site grid meter export for Kelmarsh wind farm -- `Grid Meter Energy Export (kWh)`, the "
        "real metered generation at the farm's grid connection point, plus Greenbyte's own availability "
        "flags. Ground truth for validating PECD's onshore wind capacity factor (zone UK03) against real "
        "generation; see `page_kelmarsh_vs_pecd` in energy-insights."
    ),
    metadata={
        "source": _SOURCE,
        "source_url": MetadataValue.url(_SOURCE_URL),
        "region": "UK (Kelmarsh, Northamptonshire) -- falls in PECD onshore wind zone UK03",
        "resolution": "10-minute",
        "unit": "kWh (Energy Export and other energy columns); dimensionless 0/1 for the availability flags",
        "timestamp_timezone": "naive, represents UTC (source states UTC explicitly)",
        "update_pattern": (
            "full_refresh: re-downloads and overwrites the whole series every run -- fixed archival record "
            "(2016-01-01 to 2021-07-01), not an ongoing feed"
        ),
    },
)
def kelmarsh_grid_meter(context: AssetExecutionContext) -> None:
    df = download_grid_meter()
    output_file = kelmarsh_grid_meter_file()
    df.to_parquet(output_file)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "min_timestamp": MetadataValue.text(str(df.index.min())),
            "max_timestamp": MetadataValue.text(str(df.index.max())),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(df.tail(5).to_markdown()),
        }
    )


@asset(
    group_name="kelmarsh",
    kinds={"api", "parquet"},
    tags={"load_pattern": "full_refresh"},
    description=(
        "10-minute per-turbine SCADA for Kelmarsh wind farm's 6 Senvion MM92 units -- real nacelle wind "
        "speed (plain + density-adjusted) and each turbine's own metered power, long format. Powers "
        "`page_kelmarsh_windspeed_reconstruction` in energy-insights: real wind speed through a real "
        "MM92/2050 power curve (`windpowerlib`) reconstructs Kelmarsh's actual generation far more closely "
        "than PECD's weather grid does (hourly NMAE 7.3% vs. 35.8%, both availability-adjusted)."
    ),
    metadata={
        "source": _SOURCE,
        "source_url": MetadataValue.url(_SOURCE_URL),
        "region": "UK (Kelmarsh, Northamptonshire) -- falls in PECD onshore wind zone UK03",
        "resolution": "10-minute",
        "unit": "m/s (wind speed columns); kW (Power); dimensionless 0/1 (Data Availability)",
        "timestamp_timezone": "naive, represents UTC (source states UTC explicitly)",
        "update_pattern": (
            "full_refresh: re-downloads and overwrites the whole series every run -- fixed archival record "
            "(2016-01-03 to 2021-06-30), not an ongoing feed. Slow: parses ~1.5 GB of raw per-year zips in "
            "memory, keeping only 4 of ~250 source columns -- see edh/kelmarsh.py module docstring."
        ),
    },
)
def kelmarsh_turbine_scada(context: AssetExecutionContext) -> None:
    df = download_turbine_scada()
    output_file = kelmarsh_turbine_scada_file()
    df.to_parquet(output_file, index=False)

    context.add_output_metadata(
        {
            "dagster/row_count": len(df),
            "n_turbines": df["turbine"].nunique(),
            "min_timestamp": MetadataValue.text(str(df["timestamp"].min())),
            "max_timestamp": MetadataValue.text(str(df["timestamp"].max())),
            "path": MetadataValue.path(str(output_file)),
            "preview": MetadataValue.md(df.tail(5).to_markdown()),
        }
    )


kelmarsh_assets = [kelmarsh_wt_static, kelmarsh_grid_meter, kelmarsh_turbine_scada]
