from enum import Enum


class NodeType(Enum):
    file = 1
    directory = 2


class Status(Enum):
    ok = 200,
    not_found = 404,
    error = 500,
    already_exists = 409
