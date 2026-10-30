"""Command monitoring for live PyMongo contract tests."""

from typing import Any

from pymongo.monitoring import CommandListener

class FindCommandRecorder(CommandListener):
    """A PyMongo command listener that records the issued find commands."""

    def __init__(self):
        self.find_commands: list[dict[str, Any]] = []

    def started(self, event):
        if event.command_name == "find":
            self.find_commands.append(dict(event.command))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass
