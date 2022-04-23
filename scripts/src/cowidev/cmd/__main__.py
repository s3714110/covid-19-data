import click

from cowidev.utils.params import CONFIG
from cowidev.utils.log import get_logger
from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.cmd.testing import click_test
from cowidev.cmd.vax import click_vax
from cowidev.cmd.hosp import click_hosp
from cowidev.cmd.jhu import click_jhu
from cowidev.cmd.xm import click_xm
from cowidev.cmd.gmobility import click_gm
from cowidev.cmd.variants import click_variants
from cowidev.megafile.generate import generate_megafile


@click.group(name="cowid", cls=OrderedGroup)
@click.option(
    "--parallel/--no-parallel",
    default=CONFIG.execution.parallel,
    help="Parallelize process.",
    show_default=True,
)
@click.option(
    "--n-jobs",
    default=CONFIG.execution.njobs,
    type=int,
    help="Number of threads to use.",
    show_default=True,
)
@click.option(
    "--server-mode/--no-server-mode",
    "-S",
    default=False,
    help="Only critical log and final message to slack.",
    show_default=True,
)
@click.pass_context
def cli(ctx, parallel, n_jobs, server_mode):
    """COVID-19 Data pipeline tool by Our World in Data."""
    ctx.ensure_object(dict)
    ctx.obj["parallel"] = parallel
    ctx.obj["n_jobs"] = n_jobs
    ctx.obj["server_mode"] = server_mode
    if ctx.obj['server_mode']:
        ctx.obj["logger"] = get_logger("critical")
    else:
        ctx.obj["logger"] = get_logger()


@click.command(name="megafile")
def cli_export():
    """COVID-19 data integration pipeline (former megafile)"""
    generate_megafile()


cli.add_command(click_test)
cli.add_command(click_vax)
cli.add_command(click_hosp)
cli.add_command(click_jhu)
cli.add_command(click_variants)
cli.add_command(click_xm)
cli.add_command(click_gm)
cli.add_command(cli_export)


if __name__ == "__main__":
    cli()
