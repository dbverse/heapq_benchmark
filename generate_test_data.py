import csv
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Output file path
output_file = 'C:/Users/Champ/OneDrive/Documents/Dinesh/DE/Woven_Toyota/test_data.txt'
num_records = 1000000  # 1 million records

# Create a set to store unique identifiers
unique_identifiers = set()

# Function to generate a unique identifier
def generate_unique_identifier():
    while True:
        identifier = fake.random_number(digits=10, fix_len=True)
        if identifier not in unique_identifiers:
            unique_identifiers.add(identifier)
            return identifier

# Open the output file in write mode
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=' ')
    
    # Generate and write records
    for _ in range(num_records):
        identifier = generate_unique_identifier()
        value = random.randint(1, 1000000)  # generating random values between 1 and 1,000,000
        writer.writerow([identifier, value])

print(f"Generated {num_records} records and saved to {output_file}")
