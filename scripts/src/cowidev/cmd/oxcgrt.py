import click

from cowidev import PATHS
from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.cmd.commons.utils import StepReport
from cowidev.utils.utils import get_traceback
from cowidev.oxcgrt.etl import run_etl
from cowidev.oxcgrt.grapher import run_grapheriser, run_db_updater


@click.group(name="oxcgrt", chain=True, cls=OrderedGroup)
@click.pass_context
def click_oxcgrt(ctx):
    """OxCGRT stringency index data."""
    pass


@click.command(name="get", short_help="Step 1: Download OxCGRT data.")
@click.pass_context
def click_oxcgrt_get(ctx):
    """Downloads all OxCGRT source files into project directory."""
    try:
        run_etl(PATHS.INTERNAL_INPUT_BSG_FILE, PATHS.INTERNAL_INPUT_BSG_DIFF_FILE)
    except Exception as err:
        if ctx.obj["server"]:
            StepReport(
                title="OxCGRT - [get] step failed",
                trace=get_traceback(err),
                type="error",
            ).to_slack()
        else:
            raise err


@click.command(name="grapher-io", short_help="Step 2: Generate grapher-ready files.")
@click.pass_context
def click_oxcgrt_grapher(ctx):
    try:
        run_grapheriser(
            PATHS.INTERNAL_INPUT_BSG_FILE, PATHS.INTERNAL_INPUT_BSG_STD_FILE, PATHS.INTERNAL_GRAPHER_BSG_FILE
        )
    except Exception as err:
        if ctx.obj["server"]:
            StepReport(
                title="OxCGRT - [grapher-io] step failed",
                trace=get_traceback(err),
                type="error",
            ).to_slack()
        else:
            raise err
    else:
        if ctx.obj["server"]:
            StepReport(
                title="OxCGRT - [grapher-io] step ran successfully",
                text="Grapher files were correctly generated.",
                type="success",
            ).to_slack()


@click.command(name="grapher-db", short_help="Step 3: Update Grapher database with generated files.")
def click_oxcgrt_db(ctx):
    try:
        run_db_updater(PATHS.INTERNAL_GRAPHER_BSG_FILE)
    except Exception as err:
        if ctx.obj["server"]:
            StepReport(
                title="OxCGRT - [grapher-db] step failed",
                trace=get_traceback(err),
                type="error",
            ).to_slack()
        else:
            raise err
    else:
        if ctx.obj["server"]:
            StepReport(
                title="OxCGRT - [grapher-db] step ran successfully",
                text="Data correctly uploaded to the database.",
                type="success",
            ).to_slack()


click_oxcgrt.add_command(click_oxcgrt_get)
click_oxcgrt.add_command(click_oxcgrt_grapher)
click_oxcgrt.add_command(click_oxcgrt_db)
