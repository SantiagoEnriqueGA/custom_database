from .record import Record

class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.records = []
        self.next_id = 1

    def insert(self, data, transaction=None):
        record = Record(self.next_id, data)
        if transaction:
            self.records.append(record)
            transaction.add_operation(lambda: self.records.append(record))
        else:
            self.records.append(record)
        self.next_id += 1

    def delete(self, record_id, transaction=None):
        if transaction:
            transaction.add_operation(lambda: self._delete(record_id))
        else:
            self._delete(record_id)

    def _delete(self, record_id):
        self.records = [record for record in self.records if record.id != record_id]

    def update(self, record_id, data, transaction=None):
        if transaction:
            transaction.add_operation(lambda: self._update(record_id, data))
        else:
            self._update(record_id, data)

    def _update(self, record_id, data):
        for record in self.records:
            if record.id == record_id:
                record.data = data

    def select(self, condition):
        return [record for record in self.records if condition(record)]
    
    def print_table(self):
        for record in self.records:
            print(f"Record ID: {record.id}, Data: {record.data}")