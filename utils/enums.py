class NodeType:
    file = 1
    directory = 2

    @staticmethod
    def description(stat):
        if stat == NodeType.file:
            return "file"
        else:
            return "directory"


class Status:
    ok = 200
    not_found = 404
    error = 500
    already_exists = 409

    @staticmethod
    def description(stat):
        if stat == Status.ok:
            return "Ok"
        elif stat == Status.not_found:
            return "Item not found"
        elif stat == Status.error:
            return "Internal error"
        else:
            return "Item already exists"
