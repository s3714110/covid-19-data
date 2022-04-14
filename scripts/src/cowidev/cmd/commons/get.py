import time
import importlib
from datetime import datetime

from joblib import Parallel, delayed
import pandas as pd

from cowidev.utils.log import get_logger
from cowidev.utils.utils import export_timestamp, get_traceback
from cowidev.utils.s3 import obj_from_s3

# S3 paths
LOG_MACHINES = "s3://covid-19/log/machines.json"
LOG_GET_COUNTRIES = "s3://covid-19/log/{}-get-data-countries.csv"
LOG_GET_GLOBAL = "s3://covid-19/log/{}-get-data-global.csv"


class CountryDataGetter:
    def __init__(self, modules_skip: list = [], log_header: str = ""):
        self.modules_skip = modules_skip
        self.log_header = log_header

    def _skip_module(self, module_name):
        return module_name in self.modules_skip

    def run(self, module_name: str, logger, num_retries: int = 2):
        t0 = time.time()
        # Check country skipping
        if self._skip_module(module_name):
            logger.info(f"{self.log_header} - {module_name}: skipped! ⚠️")
            return {"module_name": module_name, "success": None, "skipped": True, "time": None, "error": ""}
        # Start country scraping
        logger.info(f"{self.log_header} - {module_name}: started")
        module = importlib.import_module(module_name)
        for i in range(num_retries):
            try:
                module.main()
            except Exception as err:
                logger.info(f"{self.log_header} - {module_name}: Attempt #{i+1} failed")
                success = False
                error_msg = get_traceback(err)
            else:
                success = True
                error_msg = ""
                break
        if success:
            logger.info(f"{self.log_header} - {module_name}: SUCCESS ✅")
        else:
            logger.warning(
                f"{self.log_header} - {module_name}: ❌ FAILED after {i+1} tries: {error_msg}", exc_info=True
            )
        t = round(time.time() - t0, 2)
        return {"module_name": module_name, "success": success, "skipped": False, "time": t, "error": error_msg}


def main_get_data(
    modules: list,
    modules_valid: list,
    parallel: bool = False,
    n_jobs: int = -2,
    modules_skip: list = [],
    log_header: str = "",
    log_s3_path=None,
    output_status: str = None,
    output_status_ts: str = None,
    logging_mode: str = "info",
):
    """Get data from sources and export to output folder.

    Is equivalent to script `run_python_scripts.py`
    """
    # Logger
    logger = get_logger(logging_mode)
    t0 = time.time()
    logger.info("-- Getting data... --")
    country_data_getter = CountryDataGetter(modules_skip, log_header)
    if log_s3_path:
        modules = _load_modules_order(modules, log_s3_path)
    if parallel:
        modules_execution_results = Parallel(n_jobs=n_jobs, backend="threading")(
            delayed(country_data_getter.run)(module_name, logger) for module_name in modules
        )
    else:
        modules_execution_results = []
        for module_name in modules:
            modules_execution_results.append(country_data_getter.run(module_name, logger))
    t_sec_1 = round(time.time() - t0, 2)
    # Get timing dataframe
    df_exec = _build_df_execution(modules_execution_results)
    # Retry failed modules
    retry_log, error_log, modules_execution_results_retry = _retry_modules_failed(
        modules_execution_results, country_data_getter
    )
    logger.warning(retry_log)
    if error_log is not None:
        logger.error(error_log)
    # Status
    if output_status is not None:
        modules_execution_results += modules_execution_results_retry
        export_status(modules_execution_results, modules_valid, output_status, output_status_ts)
    # Print timing details
    t_sec_1, t_min_1, t_sec_2, t_min_2, timing_log = _print_timing(t0, t_sec_1, df_exec)
    # summary_log = summary_log_1 + summary_log_2
    logger.info(f"{timing_log}")


def export_status(modules_execution_results, modules_valid, output_status, output_status_ts):
    # Get status of executed scripts
    df_status = _build_df_status(modules_execution_results)
    # Load current status
    df_status_now = pd.read_csv(output_status)
    msk = ~df_status_now.module.isin(df_status.module)
    df_status_now = df_status_now[msk]
    # Merge
    df_status = pd.concat([df_status, df_status_now], ignore_index=True).sort_values("module")

    # Filter only running modules & set index
    df_status = df_status[df_status.module.isin(modules_valid)].set_index("module")

    # Export
    df_status.to_csv(output_status)
    export_timestamp(output_status_ts)


def _build_df_execution(modules_execution_results):
    df_exec = (
        pd.DataFrame(
            [
                {"module": m["module_name"], "execution_time (sec)": m["time"], "success": m["success"]}
                for m in modules_execution_results
            ]
        )
        .set_index("module")
        .sort_values(by="execution_time (sec)", ascending=False)
    )
    return df_exec


def _build_df_status(modules_execution_results):
    df_exec = (
        pd.DataFrame(
            [
                {
                    "module": m["module_name"],
                    "execution_time (sec)": m["time"],
                    "success": m["success"],
                    "timestamp": datetime.utcnow().replace(microsecond=0).isoformat(),
                    "error": m["error"],
                }
                for m in modules_execution_results
            ]
        )
        .sort_values(by="timestamp", ascending=True)
        .drop_duplicates(subset=["module"], keep="last")
        .sort_values(by="execution_time (sec)", ascending=False)
    )
    return df_exec


def _retry_modules_failed(modules_execution_results, country_data_getter):
    modules_failed = [m["module_name"] for m in modules_execution_results if m["success"] is False]
    retried_str = "\n".join([f"* {m}" for m in modules_failed])
    retry_log = f"""RETRIES ({len(modules_failed)})

The following scripts were re-executed:
{retried_str}
--------------------------------------
"""
    modules_execution_results = []
    for module_name in modules_failed:
        modules_execution_results.append(country_data_getter.run(module_name))
    modules_failed_retrial = [m["module_name"] for m in modules_execution_results if m["success"] is False]
    if len(modules_failed_retrial) > 0:
        failed_str = "\n".join([f"* {m}" for m in modules_failed_retrial])
        error_log = f"""FAILED ({len(modules_failed_retrial)})

The following scripts failed:
{failed_str}
--------------------------------------
"""
    else:
        error_log = None
    return retry_log, error_log, modules_execution_results


def _print_timing(t0, t_sec_1, df_time):
    t_min_1 = round(t_sec_1 / 60, 2)
    t_sec_2 = round(time.time() - t0, 2)
    t_min_2 = round(t_sec_2 / 60, 2)
    summary_log = f"""TIMING DETAILS

* Took {t_sec_1} seconds (i.e. {t_min_1} minutes).
* Top 20 most time consuming scripts:
{df_time[["execution_time (sec)"]].head(20)}
* Took {t_sec_2} seconds (i.e. {t_min_2} minutes) [AFTER RETRIALS].
--------------------------------------
"""
    return t_sec_1, t_min_1, t_sec_2, t_min_2, summary_log


def _load_modules_order(modules_name, path_log):
    if len(modules_name) < 10:
        return modules_name
    df = obj_from_s3(path_log)
    # Filter by machine
    # details = system_details()
    # machine = details["id"]
    # if machine in df.machine:
    #     df = df[df.machine == machine]
    # df = pd.read_csv(os.path.join(PATHS.INTERNAL_OUTPUT_VAX_LOG_DIR, "get-data.csv"))
    module_order_all = (
        df.sort_values("date")
        .drop_duplicates(subset=["module"], keep="last")
        .sort_values("execution_time (sec)", ascending=False)
        .module.tolist()
    )
    modules_name_order = [m for m in module_order_all if m in modules_name]
    missing = [m for m in modules_name if m not in modules_name_order]
    return modules_name_order + missing


# def _export_log_info(df_exec, t_sec_1, t_sec_2):
#     # print(len(df_new), len(MODULES_NAME), len(df_new) == len(MODULES_NAME))
#     if len(df_exec) == len(MODULES_NAME):
#         print("EXPORTING LOG DETAILS")
#         details = system_details()
#         date_now = localdate(force_today=True)
#         machine = details["id"]
#         # Export timings per country
#         df_exec = df_exec.reset_index().assign(date=date_now, machine=machine)
#         df = obj_from_s3(LOG_GET_COUNTRIES)
#         df = df[df.date + df.machine != date_now + machine]
#         df = pd.concat([df, df_exec])
#         obj_to_s3(df, LOG_GET_COUNTRIES)
#         # Export machine info
#         data = obj_from_s3(LOG_MACHINES)
#         if machine not in data:
#             data = {**details, machine: details["info"]}
#             obj_to_s3(data, LOG_MACHINES)
#         # Export overall timing
#         report = {"machine": machine, "date": date_now, "t_sec": t_sec_1, "t_sec_retry": t_sec_2}
#         df_new = pd.DataFrame([report])
#         df = obj_from_s3(LOG_GET_GLOBAL)
#         df = df[df.date + df.machine != date_now + machine]
#         df = pd.concat([df, df_new])
#         obj_to_s3(df, LOG_GET_GLOBAL)
