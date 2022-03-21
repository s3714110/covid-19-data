import os

from ._config_loader import countries_mapping

__CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))

# EMRO
__EMRO_CONFIG = os.path.join(__CURRENT_DIR, "emro_config.yaml")
EMRO_COUNTRIES = countries_mapping(__EMRO_CONFIG)

