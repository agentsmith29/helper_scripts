from typing import Dict, Any

class SlurmData:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def __getitem__(self, item):
        return self._data.get(item)

    def __getattr__(self, item):
        try:
            return self._data[item]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    def update(self, new_data: Dict[str, Any]):
        changes = {}
        for key, value in new_data.items():
            if self._data.get(key) != value:
                changes[key] = (self._data.get(key), value)
                self._data[key] = value
        return changes


class SlurmNode(SlurmData):
    pass

class SlurmMeta(SlurmData):
    pass

class SlurmErrors(SlurmData):
    pass
