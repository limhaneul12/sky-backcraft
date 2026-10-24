"""Closure parity must detect weakened action/unknown-property validation."""

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from final_closure_verify import original_tagged_schema


class McpSchemaParityTests(unittest.TestCase):
    def test_projection_restores_original_only_after_exact_closure_checks(self):
        original = {
            "type": "object", "oneOf": [
                {"properties": {"action": {"const": "list"}},
                 "required": ["action"], "additionalProperties": False},
                {"properties": {"action": {"const": "get"}, "id": {"type": "string"}},
                 "required": ["action", "id"], "additionalProperties": False},
            ],
        }
        projected = copy.deepcopy(original)
        projected.update(properties={"action": {}, "id": {}}, additionalProperties=False)
        for branch in projected["oneOf"]:
            del branch["additionalProperties"]
        projected["oneOf"][0]["not"] = {"anyOf": [{"required": ["id"]}]}
        self.assertEqual(original_tagged_schema(projected), original)
        self.assertEqual(original_tagged_schema(original), original)
        malformed = []
        for field, value in (("additionalProperties", True), ("properties", {"action": {}})):
            candidate = copy.deepcopy(projected)
            candidate[field] = value
            malformed.append(candidate)
        for predicate in (None, {"required": ["id"]}, {"anyOf": [{"required": ["other"]}]}):
            candidate = copy.deepcopy(projected)
            if predicate is None:
                del candidate["oneOf"][0]["not"]
            else:
                candidate["oneOf"][0]["not"] = predicate
            malformed.append(candidate)
        candidate = copy.deepcopy(projected)
        candidate["oneOf"][1]["not"] = {"required": ["action"]}
        malformed.append(candidate)
        for candidate in malformed:
            with self.subTest(schema=candidate), self.assertRaises(RuntimeError):
                original_tagged_schema(candidate)


if __name__ == "__main__":
    unittest.main()
