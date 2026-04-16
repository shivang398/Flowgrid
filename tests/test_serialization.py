import unittest
import json
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from common.serialization.serializer import serialize, deserialize

class Color(Enum):
    RED = "red"
    BLUE = "blue"

@dataclass
class NestedData:
    name: str
    values: List[int]

@dataclass
class ComplexData:
    id: int
    color: Color
    nested: NestedData
    extra: Dict[str, Any] = field(default_factory=dict)

class TestSerializationEdgeCases(unittest.TestCase):
    def test_complex_types(self):
        obj = ComplexData(
            id=123,
            color=Color.BLUE,
            nested=NestedData(name="test", values=[1, 2, 3]),
            extra={"a": 1.5, "b": [None, True, "hello"]}
        )
        
        # Serialize
        serialized = serialize(obj)
        data = json.loads(serialized)
        
        # Check structure
        self.assertEqual(data["id"], 123)
        self.assertEqual(data["color"], "blue")
        self.assertEqual(data["nested"]["name"], "test")
        self.assertEqual(data["nested"]["values"], [1, 2, 3])
        self.assertEqual(data["extra"]["b"][0], None)
        self.assertEqual(data["extra"]["b"][1], True)

    def test_exception_serialization(self):
        try:
            raise ValueError("Something went wrong")
        except Exception as e:
            serialized = serialize({"error": e})
            data = json.loads(serialized)
            self.assertEqual(data["error"], "Something went wrong")

    def test_deep_nesting(self):
        obj = {"a": {"b": {"c": {"d": 1}}}}
        serialized = serialize(obj)
        self.assertEqual(deserialize(serialized), obj)

    def test_tuple_to_list(self):
        # JSON does not have tuples, so we expect lists on roundtrip
        obj = {"data": (1, 2, 3)}
        serialized = serialize(obj)
        result = deserialize(serialized)
        self.assertEqual(result["data"], [1, 2, 3])

if __name__ == "__main__":
    unittest.main()
