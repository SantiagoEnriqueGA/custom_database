import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Create a new database
db = Database("MyTestDB")


# Create a new base table
# ----------------------------------------------------------------------------------
db.create_table("BaseRecords", ["name", "email"])

# Insert a record
BaseRecords = db.get_table("BaseRecords")
BaseRecords.insert({"name": "John1 Doe", "email": "john@example.com"})
BaseRecords.insert({"name": "John2 Doe", "email": "john@example.com"})
BaseRecords.insert({"name": "John3 Doe", "email": "john@example.com"})

print("\n" + "-" * 80)
print("BaseRecords Table:")
BaseRecords.print_table()


# Create a new table for VectorRecords
# ----------------------------------------------------------------------------------
db.create_table("VectorRecords", ["vector"])

# Insert vector records
VectorRecords = db.get_table("VectorRecords")
VectorRecords.insert({"vector": [1.0, 2.0, 3.0]}, record_type=VectorRecord)
VectorRecords.insert({"vector": [4.0, 5.0, 6.0]}, record_type=VectorRecord)
VectorRecords.insert({"vector": [7.0, 8.0, 9.0]}, record_type=VectorRecord)
VectorRecords.insert({"vector": [5.0, 5.0, 5.0]}, record_type=VectorRecord)

print("\n" + "-" * 80)
print("VectorRecords Table:")
VectorRecords.print_table()

vr1 = VectorRecords.records[0]

# Example usage of VectorRecord methods
print(f"\nExample usage of VectorRecord Methods on record: {vr1.vector}:")
print(f"VectorRecord Magnitude: {vr1.magnitude():.4f}")
print(f"VectorRecord Normalized: {[round(x, 4) for x in vr1.normalize()]}")
print(f"VectorRecord Dot Product: {vr1.dot_product([1.0, 1.0, 1.0]):.4f}")


# Create a new table for TimeSeriesRecords
# ----------------------------------------------------------------------------------
db.create_table("TimeSeriesRecords", ["time_series"])

# Insert time series records
TimeSeriesRecords = db.get_table("TimeSeriesRecords")
TimeSeriesRecords.insert({"time_series": [1, 2, 3, 4]}, record_type=TimeSeriesRecord)
TimeSeriesRecords.insert({"time_series": [5, 6, 7, 8]}, record_type=TimeSeriesRecord)

print("\n" + "-" * 80)
print("TimeSeriesRecords Table:")
TimeSeriesRecords.print_table()

time1 = TimeSeriesRecords.records[0]

# Example usage of TimeSeriesRecord methods
print(f"\nExample usage of TimeSeriesRecord Methods on record: {time1.time_series}:")
print(f"TimeSeriesRecord Moving Average (window_size=2): {[round(x, 4) for x in time1.moving_average(2)]}")
print(f"TimeSeriesRecord Moving Average (window_size=3): {[round(x, 4) for x in time1.moving_average(3)]}")



# Create a new table for TextRecords
# ----------------------------------------------------------------------------------
db.create_table("TextRecords", ["text"])

# Insert text records
TextRecords = db.get_table("TextRecords")
TextRecords.insert({"text": "Hello, world!"}, record_type=TextRecord)
TextRecords.insert({"text": "Goodbye, world!"}, record_type=TextRecord)

print("\n" + "-" * 80)
print("TextRecords Table:")
TextRecords.print_table()

text1 = TextRecords.records[0]

# Example usage of TextRecord methods
print(f"\nExample usage of TextRecord Methods on record: {text1.text}:")
print(f"TextRecord Word Count: {text1.word_count()}")
print(f"TextRecord Uppercase: {text1.to_uppercase()}")
print(f"TextRecord Lowercase: {text1.to_lowercase()}")


# Create a new table for ImageRecords
# ----------------------------------------------------------------------------------
db.create_table("ImageRecords", ["image_data"])

# Insert image records
ImageRecords = db.get_table("ImageRecords")
ImageRecords.insert({"image_data": 'example_datasets/cube.png'}, record_type=ImageRecord)

print("\n" + "-" * 80)
print("ImageRecords Table:")
ImageRecords.print_table(pretty=True)

image1 = ImageRecords.records[0]

# Example usage of ImageRecord methods
print(f"\nExample usage of ImageRecord Methods on record: {image1.image_path}, byte size: {image1.image_size}:")
print(f"ImageRecord Display:")
# image1.get_image().show()
image1.get_image()
print(f"ImageRecord Resize (50%):")
# image1.resize(0.5).show()
image1.resize(0.5)
