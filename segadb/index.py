class Index:
    def __init__(self):
        self.index = {}

    def add(self, key, record_id):
        if key not in self.index:
            self.index[key] = []
        self.index[key].append(record_id)

    def remove(self, key, record_id):
        if key in self.index:
            self.index[key].remove(record_id)
            if not self.index[key]:
                del self.index[key]

    def find(self, key):
        return self.index.get(key, [])