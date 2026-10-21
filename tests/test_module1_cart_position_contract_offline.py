"""Dependency-free cleaner-to-generator int32 cart-position contract tests."""

import ast
from pathlib import Path
import unittest

from src.module1.config import INT32_MAX, validate_cart_position_range


ROOT = Path(__file__).resolve().parents[1]


class CartPositionContractTests(unittest.TestCase):
    def test_valid_cart_positions_fit_int32_without_narrowing(self):
        for value in (32767, 32768, 70000, INT32_MAX):
            with self.subTest(value=value):
                self.assertEqual(validate_cart_position_range(value, value), (value, value))

    def test_invalid_cart_position_ranges_fail_before_serialization(self):
        for minimum, maximum in (
            (0, 1),
            (-1, 1),
            (1, INT32_MAX + 1),
            (1.0, 1),
            (1, True),
            (2, 1),
        ):
            with self.subTest(minimum=minimum, maximum=maximum), self.assertRaises(ValueError):
                validate_cart_position_range(minimum, maximum)

    def test_generator_validates_bounds_and_serializes_cart_positions_as_int32(self):
        tree = ast.parse((ROOT / "src/module1/generate.py").read_text(encoding="utf-8"))
        finalize = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "finalize"
        )
        calls = [node for node in ast.walk(finalize) if isinstance(node, ast.Call)]
        self.assertTrue(any(
            isinstance(call.func, ast.Name) and call.func.id == "validate_cart_position_range"
            for call in calls
        ))
        self.assertTrue(any(
            isinstance(call.func, ast.Attribute)
            and call.func.attr == "is_integer_dtype"
            for call in calls
        ))
        cart_assignment = next(
            node for node in finalize.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name) and target.value.id == "output_df"
                and isinstance(target.slice, ast.Constant) and target.slice.value == "add_to_cart_order"
                for target in node.targets
            )
        )
        cast = cart_assignment.value
        self.assertIsInstance(cast, ast.Call)
        self.assertIsInstance(cast.func, ast.Attribute)
        self.assertEqual(cast.func.attr, "astype")
        self.assertEqual(cast.args[0].value, "int32")

        event_schema = ast.parse((ROOT / "src/module3/parse.py").read_text(encoding="utf-8"))
        schema_calls = [node for node in ast.walk(event_schema) if isinstance(node, ast.Call)]
        self.assertTrue(any(
            isinstance(call.func, ast.Name) and call.func.id == "StructField"
            and len(call.args) >= 2
            and isinstance(call.args[0], ast.Constant) and call.args[0].value == "add_to_cart_order"
            and isinstance(call.args[1], ast.Call)
            and isinstance(call.args[1].func, ast.Name) and call.args[1].func.id == "IntegerType"
            for call in schema_calls
        ))


if __name__ == "__main__":
    unittest.main()
