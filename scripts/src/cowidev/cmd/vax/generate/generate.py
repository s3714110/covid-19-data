import click

from cowidev.cmd.vax.generate.utils import DatasetGenerator
from cowidev.cmd.commons.utils import StepReport
from cowidev.utils.log import get_logger
from cowidev.utils.utils import get_traceback


@click.option(
    "--server-mode/--no-server-mode",
    "-O",
    default=False,
    help="Only critical log and final message to slack.",
    show_default=True,
)
@click.command(name="generate", short_help="Step 3: Generate vaccination dataset.")
def click_vax_generate(server_mode):
    # Get logger
    if server_mode:
        logger = get_logger("critical")
    else:
        logger = get_logger()
    # Select columns
    generator = DatasetGenerator(logger)
    try:
        generator.run()
    except Exception as err:
        if server_mode:
            StepReport(
                title="Vaccinations - [generate] step failed",
                trace=get_traceback(err),
                type="error",
            ).to_slack()
        else:
            raise err
    else:
        if server_mode:
            StepReport(
                title="Vaccinations - [generate] step ran successfully",
                text="Intermediate vaccinations files were correctly generated.",
                type="success",
            ).to_slack()
