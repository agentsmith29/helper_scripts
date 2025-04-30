import json
import re

from SlurmNodes import SlurmNode, SlurmMeta, SlurmErrors

class Formatter():
    def __init__(self):
        self._data = None

    def format(self, template: str) -> str:
        """
        Formats a string like "{meta.plugin.type}" or "{nodes[0].architecture}".
        """
        if self._data is None:
            raise ValueError("Data not set. Please set data before formatting.")

        def repl(match):
                path = match.group(1).strip()
                parts = re.split(r'[\.\[\]]+', path)
                parts = [p for p in parts if p]

                obj = self
                for p in parts:
                    if isinstance(obj, list):
                        p = int(p)
                        obj = obj[p]
                    elif isinstance(obj, (SlurmNode, SlurmMeta, SlurmErrors)):
                        if p in obj._data:
                            obj = obj._data[p]
                        else:
                            raise AttributeError(f"'{type(obj).__name__}' has no key '{p}'")
                    elif isinstance(obj, dict):
                        obj = obj.get(p)
                    else:
                        raise TypeError(f"Cannot descend into {type(obj).__name__} with key '{p}'")
                return str(obj)

        return re.sub(r'\{([^{}]+)\}', repl, template)




class SlurmParser_sinfo(Formatter):

    def __init__(self, sinfo_json: str):
        self._sinfo_json = sinfo_json
        self._data = json.loads(data)
        self.meta = SlurmMeta(self._data.get('meta', {}))
        self.errors = [SlurmErrors(e) for e in self._data.get('errors', [])]
        self.nodes = [SlurmNode(n) for n in self._data.get('nodes', [])]


    @property
    def data(self):
        return self._data

    def update(self, data: str):
        """ Update the sinfo json """
        self._sinfo_json = data
        self._data = json.loads(data)


if __name__ == "__main__":
    # read the file "tmp",l open it and convert it to a json object
    with open("tmp", "r") as f:
        data = f.read()

    sp_sinfo = SlurmParser_sinfo(data)

    # print(sp_sinfo.data)
    print(sp_sinfo.format("I want to have {nodes[0].name}"))

