import sys
from .logger import logger


class Storage:
    def __init__(self, operation_log: list[dict[str, any]]):
        self._table: dict[str, any] = {}
        for entry in operation_log:
            _ = self.apply(entry['operation'])

    def apply(self, operation):
        key = operation['key']
        if operation['type'] == 'create':
            if key in self._table:
                return {'status': 'already exists', 'value': self._table[key]}
            self._table[key] = operation['value']
            return {'status': 'successfully created'}
        if operation['type'] == 'read':
            if key in self._table:
                return {'status': 'successfully read', 'value': self._table[key]}
            return {'status': 'did not exist'}
        if operation['type'] == 'update':
            if key in self._table:
                previous_value = self._table[key]
                self._table[key] = operation['value']
                return {'status': 'successfully updated', 'previous_value': previous_value}
            return {'status': 'did not exist'}
        if operation['type'] == 'delete':
            if key in self._table:
                value = self._table[key]
                self._table.pop(key)
                return {'status': 'successfully deleted', 'value': value}
            return {'status': 'did not exist'}
        if operation['type'] == 'cas':
            # Even works with (key not in _table) and with (expected is null)
            expected = operation['expected']
            desired = operation['desired']
            if self._table.get(key, None) == expected:
                self._table[key] = desired
                return {'status': 'successfully exchanged'}
            return {'status': 'actual and desired value did not match', 'actual': self._table.get(key, None)}
        logger.fatal(f'apply method has not returned anything, operation: {operation}')
        sys.exit(1)
