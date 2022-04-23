import click

from cowidev.cmd.commons.utils import OrderedGroup
from cowidev.jhu.__main__ import download_csv, main, update_db


@click.group(name="jhu", chain=True, cls=OrderedGroup)
@click.pass_context
def click_jhu(ctx):
    """COVID-19 Cases/Deaths data pipeline."""
    pass


@click.command(name="get", short_help="Step 1: Download JHU data.")
@click.pass_context
def click_jhu_download(ctx):
    """Downloads all JHU source files into project directory."""
    download_csv(ctx.obj["logger"])


@click.command(name="generate", short_help="Step 2: Generate dataset.")
@click.pass_context
def click_jhu_generate(ctx):
    main(ctx.obj["logger"], skip_download=True)


@click.command(name="grapher-db", short_help="Step 3: Update Grapher database with generated files.")
def click_jhu_db():
    update_db()


click_jhu.add_command(click_jhu_download)
click_jhu.add_command(click_jhu_generate)
click_jhu.add_command(click_jhu_db)
