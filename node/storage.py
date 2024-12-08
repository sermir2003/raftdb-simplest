import sys
from .logger import logger


class Storage:
    def __init__(self, changelog):
        self._table = dict()
        for entry in changelog:
            _ = self.apply(entry)

    def apply(self, operation):
        if operation['type'] == 'create':
            key = operation['key']
            if key in self._table:
                return {'message': 'already exists'}
            self._table[key] = operation['value']
            return {'message': 'ok'}
        if operation['type'] == 'read':
            key = operation['key']
            if key in self._table:
                return {'message': 'ok', 'value': self._table[key]}
            return {'message': 'not found'}
        if operation['type'] == 'update':
            key = operation['key']
            if key in self._table:
                self._table[key] = operation['value']
                return {'message': 'ok'}
            return {'message': 'does not exist'}
        if operation['type'] == 'delete':
            key = operation['key']
            if key in self._table:
                self._table.pop(key)
                return {'message': 'ok'}
            return {'message': 'not found'}
        if operation['type'] == 'cas':
            key = operation['key']
            if key in self._table:
                if self._table[key] == operation['expected']:
                    self._table[key] = operation['desired']
                    return {'message': 'ok'}
                return {'message': 'unsuccessful cas', 'actual': self._table[key]}
            return {'message': 'not found'}
        logger.fatal(f'Unknown operation type: {operation['type']}')
        sys.exit(1)

    def read(self, key):
        return self._table.get(key, None)
