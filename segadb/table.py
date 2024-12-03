from .record import Record

class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.records = []
        self.next_id = 1

    def insert(self, data):
        record = Record(self.next_id, data)
        self.records.append(record)
        self.next_id += 1

    def delete(self, record_id):
        self.records = [record for record in self.records if record.id != record_id]

    def update(self, record_id, data):
        for record in self.records:
            if record.id == record_id:
                record.data = data

    def select(self, condition):
        return [record for record in self.records if condition(record)]