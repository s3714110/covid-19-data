import os
from datetime import datetime

import pandas as pd
from pandas.core.base import DataError
from pandas.errors import ParserError
import click

from cowidev import PATHS
from cowidev.utils.log import get_logger, print_eoe
from cowidev.utils.params import CONFIG
from cowidev.utils.utils import export_timestamp, get_traceback
from cowidev.cmd.vax.process.utils import process_location, VaccinationGSheet


@click.option(
    "--log-only-errors/--log-all",
    "-O",
    default=False,
    help="Optimize processes based on older logging times.",
    show_default=True,
)
@click.command(name="process", short_help="Step 2: Process scraped vaccination data from primary sources.")
def click_vax_process(log_only_errors):
    """Process data in folder scripts/output/vaccinations/.

    By default, the default values for OPTIONS are those specified in the configuration file. The configuration file is
    a YAML file with the pipeline settings. Note that the environment variable `OWID_COVID_CONFIG` must be pointing to
    this file. We provide a default config file in the project folder scripts/config.yaml.

    OPTIONS passed via command line will overwrite those from configuration file.

    Example:
    Process all country data.

        cowid vax process
    """
    if log_only_errors:
        logger = get_logger("error")
    else:
        logger = get_logger()

    main_process_data(
        path_input_files=PATHS.INTERNAL_OUTPUT_VAX_MAIN_DIR,
        path_output_files=PATHS.DATA_VAX_COUNTRY_DIR,
        path_output_meta=PATHS.INTERNAL_OUTPUT_VAX_META_FILE,
        column_location="location",
        process_location=process_location,
        path_output_status=PATHS.INTERNAL_OUTPUT_VAX_STATUS_PROCESS,
        path_output_status_ts=PATHS.INTERNAL_OUTPUT_VAX_STATUS_PROCESS_TS,
        log_header="VAX",
        skip_complete=CONFIG.pipeline.vaccinations.process.skip_complete,
        skip_monotonic=CONFIG.pipeline.vaccinations.process.skip_monotonic_check,
        skip_anomaly=CONFIG.pipeline.vaccinations.process.skip_anomaly_check,
        logger=logger,
    )


def main_process_data(
    path_input_files: str,
    path_output_files: str,
    path_output_meta: str,
    column_location: str,
    process_location: callable,
    path_output_status: str,
    path_output_status_ts: str,
    log_header: str,
    logger,
    skip_complete: list = None,
    skip_monotonic: dict = {},
    skip_anomaly: dict = {},
):
    # TODO: Generalize
    logger.info("-- Processing data... --")
    # Get data from sheets (i.e. manual data)
    logger.info("Getting data from Google Spreadsheet...")
    gsheet = VaccinationGSheet()
    dfs_manual = gsheet.df_list()
    # Get automated-country data
    logger.info("Getting data from internal output...")
    automated = gsheet.automated_countries
    filepaths_auto = [os.path.join(path_input_files, f"{country}.csv") for country in automated]
    dfs_auto = [read_csv(filepath) for filepath in filepaths_auto]
    # Concatenate list of dataframes
    dfs = dfs_manual + dfs_auto

    # Check that no location is present in both manual and automated data
    _check_no_overlapping_manual_auto(dfs_manual, dfs_auto)

    # vax = [v for v in vax if v.location.iloc[0] == "Pakistan"]  # DEBUG
    # Process locations
    def _process_location_and_move_file(df):
        if column_location not in df:
            raise ValueError(f"Column `{column_location}` missing. df: {df.tail(5)}")
        country = df.loc[0, column_location]
        if country.lower() not in skip_complete:
            # Process dataframe
            monotonic_check_skip = skip_monotonic.get(df.loc[0, column_location], [])
            anomaly_check_skip = skip_anomaly.get(df.loc[0, column_location], [])
            try:
                df = process_location(df, monotonic_check_skip, anomaly_check_skip)
            except Exception as err:
                success = False
                error_msg = get_traceback(err)
                logger.error(f"{log_header} - {country}: FAILED ❌ {err}")
            except:
                success = False
                error_msg = "Error"
            else:
                # Export
                df.to_csv(os.path.join(path_output_files, f"{country}.csv"), index=False)
                logger.info(f"{country}: SUCCESS ✅")
                success = True
                error_msg = ""
            finally:
                return {
                    "location": country,
                    "success": success,
                    "skipped": False,
                    "error": error_msg,
                    "timestamp": datetime.utcnow().replace(microsecond=0).isoformat(),
                }
        else:
            logger.info(f"{country}: SKIPPED 🚧")
            return {
                "location": country,
                "success": None,
                "skipped": True,
                "error": "",
                "timestamp": datetime.utcnow().replace(microsecond=0).isoformat(),
            }
        # return {"location": country, "error": ""}

    logger.info("Processing and exporting data...")
    # Process all countries
    df_status = pd.DataFrame([_process_location_and_move_file(df) for df in dfs])

    # Export metadata
    gsheet.metadata.to_csv(path_output_meta, index=False)
    logger.info("Exported ✅")

    # Export status
    df_status.to_csv(path_output_status, index=False)
    export_timestamp(path_output_status_ts)
    print_eoe()


def read_csv(filepath):
    try:
        return pd.read_csv(filepath)
    except:
        raise ParserError(f"Error tokenizing data from file {filepath}")


def _check_no_overlapping_manual_auto(dfs_manual, dfs_auto):
    locations_manual = set([df.location[0] for df in dfs_manual])
    locations_auto = set([df.location[0] for df in dfs_auto])
    locations_common = locations_auto.intersection(locations_manual)
    if len(locations_common) > 0:
        raise DataError(f"The following locations have data in both output/main_data and GSheet: {locations_common}")
