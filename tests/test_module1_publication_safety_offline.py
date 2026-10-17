"""Dependency-free filesystem safety tests for Module 1 publication helpers."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


def _load_clean_module():
    fake_pandas = ModuleType("pandas")
    fake_pandas.DataFrame = object
    fake_pandas.errors = SimpleNamespace(ParserError=type("ParserError", (Exception,), {}))
    spec = importlib.util.spec_from_file_location(
        "module1_clean_publication_safety_test", ROOT / "src/module1/clean.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"pandas": fake_pandas}):
        spec.loader.exec_module(module)
    return module


class _ParquetFixture:
    def __init__(self, value: bytes = b"published"):
        self.value = value

    def to_parquet(self, path, index=False):
        if index is not False:
            raise AssertionError("Module 1 publication must omit DataFrame indexes")
        Path(path).write_bytes(self.value)


def _outputs(module, value=b"published"):
    return {name: _ParquetFixture(value) for name in module._CLEAN_OUTPUT_FILES}


class _SeriesFixture:
    def __init__(self, values, dtype=None):
        self.values = list(values)
        self.dtype = dtype

    def astype(self, dtype):
        return _SeriesFixture(self.values, dtype)


class _FrameFixture:
    def __init__(self, values):
        self.values = {name: value if isinstance(value, _SeriesFixture) else _SeriesFixture(value) for name, value in values.items()}

    def copy(self):
        return _FrameFixture({name: series.values for name, series in self.values.items()})

    def __getitem__(self, name):
        return self.values[name]

    def __setitem__(self, name, value):
        self.values[name] = value


def _mark_owned_output(module, path, value=b"previous"):
    path.mkdir()
    for name in module._CLEAN_OUTPUT_FILES:
        (path / name).write_bytes(value)
    (path / module._CLEAN_OUTPUT_MANIFEST).write_text(
        json.dumps({"schema_version": 1, "files": sorted(module._CLEAN_OUTPUT_FILES)}),
        encoding="utf-8",
    )


class Module1PublicationSafetyTests(unittest.TestCase):
    def setUp(self):
        self.clean = _load_clean_module()

    def test_raw_and_output_overlap_is_rejected_without_touching_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            raw = data / "raw"
            raw.mkdir(parents=True)
            source = raw / "orders.csv"
            source.write_bytes(b"raw sentinel")
            for output in (raw, data, raw / "nested" / "clean"):
                with self.subTest(output=output), self.assertRaisesRegex(ValueError, "overlap"):
                    self.clean._validate_publication_paths(data, output)
            self.assertEqual(source.read_bytes(), b"raw sentinel")

    def test_symlink_alias_is_rejected_before_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            (data / "raw").mkdir(parents=True)
            alias = root / "alias"
            alias.symlink_to(data, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlink"):
                self.clean._validate_publication_paths(data, alias / "raw" / "clean")

    def test_owned_publication_can_be_replaced_without_deleting_raw_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            data = Path(temporary) / "data"
            raw = data / "raw"
            raw.mkdir(parents=True)
            source = raw / "orders.csv"
            source.write_bytes(b"raw sentinel")
            output = data / "clean"

            self.clean._validate_publication_paths(data, output)
            self.clean._write_outputs_atomically(output, _outputs(self.clean, b"first"))
            self.clean._write_outputs_atomically(output, _outputs(self.clean, b"replacement"))

            self.assertEqual(source.read_bytes(), b"raw sentinel")
            self.assertEqual(
                {path.name for path in output.iterdir()},
                self.clean._CLEAN_OUTPUT_FILES | {self.clean._CLEAN_OUTPUT_MANIFEST},
            )
            self.assertTrue(all((output / name).read_bytes() == b"replacement" for name in self.clean._CLEAN_OUTPUT_FILES))

    def test_cart_positions_use_int32_and_preserve_large_valid_values(self):
        source = _FrameFixture({"reordered": [0, 1], "add_to_cart_order": [32768, 70000]})

        result = self.clean.clean_order_products(source)

        self.assertEqual(result["add_to_cart_order"].values, [32768, 70000])
        self.assertEqual(result["add_to_cart_order"].dtype, "int32")
        self.assertEqual(self.clean.RAW_DATA_TYPES["order_products__prior"]["add_to_cart_order"], "int32")

    def test_foreign_output_and_failed_staging_preserve_existing_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            data = Path(temporary) / "data"
            raw = data / "raw"
            raw.mkdir(parents=True)
            source = raw / "orders.csv"
            source.write_bytes(b"raw sentinel")
            output = data / "clean"
            output.mkdir()
            foreign = output / "keep-me.txt"
            foreign.write_text("foreign sentinel", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unrecognized"):
                self.clean._write_outputs_atomically(output, _outputs(self.clean))
            self.assertEqual(foreign.read_text(encoding="utf-8"), "foreign sentinel")
            self.assertEqual(source.read_bytes(), b"raw sentinel")
            foreign.unlink()
            output.rmdir()

            _mark_owned_output(self.clean, output)
            previous = {name: (output / name).read_bytes() for name in self.clean._CLEAN_OUTPUT_FILES}

            class FailingFixture(_ParquetFixture):
                def to_parquet(self, path, index=False):
                    raise OSError("synthetic staging failure")

            frames = _outputs(self.clean)
            frames[next(iter(frames))] = FailingFixture()
            with self.assertRaisesRegex(OSError, "synthetic staging failure"):
                self.clean._write_outputs_atomically(output, frames)

            self.assertEqual(source.read_bytes(), b"raw sentinel")
            self.assertEqual({name: (output / name).read_bytes() for name in previous}, previous)


if __name__ == "__main__":
    unittest.main()
