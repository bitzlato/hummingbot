import logging
from pathlib import Path

import bugsnag
from bugsnag.handlers import BugsnagHandler

from hummingbot.client.config.global_config_map import global_config_map


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


bugsnag.configure(
    api_key=global_config_map["bugsnag_api_key"].value,
    project_root=str(PROJECT_ROOT),
)


class MyBugsnagHandler(BugsnagHandler):
    def __init__(self, client=None, extra_fields=None, level: int = logging.ERROR):
        super(MyBugsnagHandler, self).__init__(client=client, extra_fields=extra_fields)
        self.setLevel(level)
