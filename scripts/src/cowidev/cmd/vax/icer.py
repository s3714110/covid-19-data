import os

import click

from cowidev.vax.icer import main


@click.command(
    name="icer", short_help="Download some specific country files. Useful when these are very large in size."
)
@click.pass_context
def click_vax_icer(ctx):
    main(ctx.obj["logger"])
