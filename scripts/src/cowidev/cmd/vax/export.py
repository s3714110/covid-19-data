import webbrowser
import pyperclip

import click

from cowidev import PATHS
from cowidev.utils.params import SECRETS
from cowidev.megafile.generate import generate_megafile
from cowidev.cmd.commons.utils import StepReport
from cowidev.utils.utils import get_traceback


@click.command(name="export", short_help="Step 4: Export vaccination data and merge with global dataset.")
@click.pass_context
def click_vax_export(ctx):
    try:
        main_source_table_html(SECRETS.vaccinations.post, ctx.obj["logger"])
        main_megafile(ctx.obj["logger"])
    except Exception as err:
        if ctx.obj["server_mode"]:
            StepReport(
                title="Vaccinations - [export] step failed",
                trace=get_traceback(err),
                type="error",
            ).to_slack()
        else:
            raise err
    else:
        if ctx.obj["server_mode"]:
            StepReport(
                title="Vaccinations - [export] step ran successfully",
                text="Megafile generated, source table updated.",
                type="success",
            ).to_slack()


def main_source_table_html(url, logger):
    # Read html content
    logger.info("-- Reading HTML table... --")
    with open(PATHS.DATA_INTERNAL_VAX_TABLE, "r") as f:
        html = f.read()
    logger.info("Redirecting to owid editing platform...")
    try:
        pyperclip.copy(html)
        webbrowser.open(url)
    except:
        logger.error(
            f"Can't copy content and open browser. Please visit {url} and copy the content from"
            f" {PATHS.DATA_INTERNAL_VAX_TABLE}"
        )


def main_megafile(logger):
    """Executes scripts/scripts/megafile.py."""
    logger.info("-- Generating megafiles... --")
    generate_megafile(logger)
