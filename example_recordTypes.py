from segadb import Database, Storage, Transaction, Record, VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord

# Create a new database
db = Database("MyTestDB")

# Create a new base table
db.create_table("BaseRecords", ["name", "email"])

# Insert a record
BaseRecords = db.get_table("BaseRecords")
BaseRecords.insert({"name": "John1 Doe", "email": "john@example.com"})
BaseRecords.insert({"name": "John2 Doe", "email": "john@example.com"})
BaseRecords.insert({"name": "John3 Doe", "email": "john@example.com"})

print("BaseRecords Table:")
BaseRecords.print_table()

# Create a new table for VectorRecords
db.create_table("VectorRecords", ["vector"])

# Insert vector records
VectorRecords = db.get_table("VectorRecords")
VectorRecords.insert({"vector": [1.0, 2.0, 3.0]}, record_type=VectorRecord)
VectorRecords.insert({"vector": [4.0, 5.0, 6.0]}, record_type=VectorRecord)

print("\nVectorRecords Table:")
VectorRecords.print_table()

# Create a new table for TimeSeriesRecords
db.create_table("TimeSeriesRecords", ["time_series"])

# Insert time series records
TimeSeriesRecords = db.get_table("TimeSeriesRecords")
TimeSeriesRecords.insert({"time_series": [1, 2, 3, 4]}, record_type=TimeSeriesRecord)
TimeSeriesRecords.insert({"time_series": [5, 6, 7, 8]}, record_type=TimeSeriesRecord)

print("\nTimeSeriesRecords Table:")
TimeSeriesRecords.print_table()

# Create a new table for ImageRecords
db.create_table("ImageRecords", ["image_data"])

# Insert image records
ImageRecords = db.get_table("ImageRecords")
ImageRecords.insert({"image_data": b'\x89PNG...'}, record_type=ImageRecord)
ImageRecords.insert({"image_data": b'\x89PNG...'}, record_type=ImageRecord)

print("\nImageRecords Table:")
ImageRecords.print_table()

# Create a new table for TextRecords
db.create_table("TextRecords", ["text"])

# Insert text records
TextRecords = db.get_table("TextRecords")
TextRecords.insert({"text": "Hello, world!"}, record_type=TextRecord)
TextRecords.insert({"text": "Goodbye, world!"}, record_type=TextRecord)

print("\nTextRecords Table:")
TextRecords.print_table()