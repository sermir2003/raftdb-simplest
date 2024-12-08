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
            else:
                self._table[key] = operation['value']
                return {'message': 'ok'}
        elif operation['type'] == 'read':
            key = operation['key']
            if key in self._table:
                return {'message': 'ok', 'value': self._table[key]}
            else:
                return {'message': 'not found'}
        elif operation['type'] == 'update':
            key = operation['key']
            if key in self._table:
                self._table[key] = operation['value']
                return {'message': 'ok'}
            else:
                return {'message': 'does not exist'}
        elif operation['type'] == 'delete':
            key = operation['key']
            if key in self._table:
                self._table.pop(key)
                return {'message': 'ok'}
            else:
                return {'message': 'not found'}
        elif operation['type'] == 'cas':
            key = operation['key']
            if key in self._table:
                if self._table[key] == operation['expected']:
                    self._table[key] == operation['desired']
                    return {'message': 'ok'}
                else:
                    return {'message': 'unsuccessful cas', 'actual': self._table[key]}
            else:
                return {'message': 'not found'}
