import json

class Storage:
    @staticmethod
    def save(database, filename):
        with open(filename, 'w') as file:
            json.dump(database, file, default=lambda o: o.__dict__, indent=4)

    @staticmethod
    def load(filename):
        with open(filename, 'r') as file:
            data = json.load(file)
            # Reconstruct the database object from JSON data
            # This part requires custom deserialization logic
            return data
        
    @staticmethod
    def delete(filename):
        import os
        os.remove(filename)
        