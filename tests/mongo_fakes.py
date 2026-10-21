"""MongoDB fakes shared by the Module 3 and Module 4 test suites.

Several test modules need the same minimal stand-ins for the PyMongo driver.
Keeping one copy means a change to the driver contract is reflected in every
test that uses it.
"""

from __future__ import annotations

from typing import Any


class FakeUpdateOne:
    """Record of an `update_one` call, as the writer would issue it."""

    def __init__(self, filter_document, update_document, upsert):
        self.filter = filter_document
        self.update = update_document
        self.upsert = upsert


class FakeBulkResult:
    """The counters PyMongo returns from a bulk write."""

    def __init__(self, upserted=0, matched=0, modified=0):
        self.upserted_count = upserted
        self.matched_count = matched
        self.modified_count = modified


class FakeClient:
    """A MongoClient stand-in that always resolves one database."""

    def __init__(self, database):
        self.database = database

    def __getitem__(self, name):
        assert name == "instacart"
        return self.database


class FindCommandRecorder:
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
