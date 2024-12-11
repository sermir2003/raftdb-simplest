import sys
from .logger import logger


class Storage:
    def __init__(self, changelog: list[dict[str, any]]):
        self._table: dict[str, any] = {}
        for entry in changelog:
            _ = self.apply(entry['change'])

    def apply(self, change):
        key = change['key']
        if change['type'] == 'create':
            if key in self._table:
                return {'status': 'already exists', 'value': self._table[key]}
            self._table[key] = change['value']
            return {'status': 'successfully created'}
        if change['type'] == 'read':
            if key in self._table:
                return {'status': 'successfully read', 'value': self._table[key]}
            return {'status': 'did not exist'}
        if change['type'] == 'update':
            if key in self._table:
                previous_value = self._table[key]
                self._table[key] = change['value']
                return {'status': 'successfully updated', 'previous_value': previous_value}
            return {'status': 'did not exist'}
        if change['type'] == 'delete':
            if key in self._table:
                value = self._table[key]
                self._table.pop(key)
                return {'status': 'successfully deleted', 'value': value}
            return {'status': 'did not exist'}
        if change['type'] == 'cas':
            # Even works with (key not in _table) and with (expected is null)
            expected = change['expected']
            desired = change['desired']
            if self._table.get(key, None) == expected:
                self._table[key] = desired
                return {'status': 'successfully exchanged'}
            return {'status': 'actual and desired value did not match', 'actual': self._table.get(key, None)}
        logger.fatal(f'apply method has not returned anything, change: {change}')
        sys.exit(1)
