import webbrowser
import pyperclip

import click

from cowidev import PATHS
from cowidev.utils.log import get_logger
from cowidev.utils.params import SECRETS
from cowidev.megafile.generate import generate_megafile

logger = get_logger()


@click.command(name="export", short_help="Step 4: Export vaccination data and merge with global dataset.")
def click_vax_export():
    main_source_table_html(SECRETS.vaccinations.post)
    main_megafile()


def main_source_table_html(url):
    # Read html content
    print("-- Reading HTML table... --")
    with open(PATHS.DATA_INTERNAL_VAX_TABLE, "r") as f:
        html = f.read()
    logger.info("Redirecting to owid editing platform...")
    try:
        pyperclip.copy(html)
        webbrowser.open(url)
    except:
        print(
            f"Can't copy content and open browser. Please visit {url} and copy the content from"
            f" {PATHS.DATA_INTERNAL_VAX_TABLE}"
        )


def main_megafile():
    """Executes scripts/scripts/megafile.py."""
    print("-- Generating megafiles... --")
    generate_megafile()
