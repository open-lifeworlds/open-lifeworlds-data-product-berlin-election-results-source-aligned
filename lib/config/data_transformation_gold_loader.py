import collections
import os
from dataclasses import dataclass, field
from typing import List, Optional

import yaml
from dacite import from_dict
from yaml import MappingNode
from yaml.constructor import ConstructorError

from lib.tracking_decorator import TrackingDecorator


@dataclass
class Name:
    name: str
    type: Optional[str] = "keep"
    concat: Optional[List[str]] = None
    numerator: Optional[str] = None
    denominator: Optional[str] = None


@dataclass
class File:
    source_file_name: str
    target_file_name: str
    names: Optional[List[Name]] = field(default_factory=list)


@dataclass
class InputPort:
    id: str
    files: Optional[List[File]] = field(default_factory=list)


@dataclass
class DataTransformation:
    input_ports: Optional[List[InputPort]] = field(default_factory=list)


class Loader(yaml.SafeLoader):
    """
    Customer loader that makes sure that some fields are read in a raw format
    """

    raw_fields = ["sheet_name"]

    def construct_mapping(self, node, deep=False):
        if not isinstance(node, MappingNode):
            raise ConstructorError(
                None,
                None,
                "expected a mapping node, but found %s" % node.id,
                node.start_mark,
            )
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, collections.abc.Hashable):
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found unhashable key",
                    key_node.start_mark,
                )

            # Make sure that some fields are read in a raw format
            if key in self.raw_fields:
                value = value_node.value
            else:
                value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping


@TrackingDecorator.track_time
def load_data_transformation_gold(config_path) -> DataTransformation:
    data_transformation_path = os.path.join(config_path, "data-transformation-03-gold.yml")

    if os.path.exists(data_transformation_path):
        with open(data_transformation_path, "r") as file:
            data = yaml.load(file, Loader=Loader)
        return from_dict(data_class=DataTransformation, data=data)
    else:
        print(f"✗️ Config file {data_transformation_path} does not exist")
