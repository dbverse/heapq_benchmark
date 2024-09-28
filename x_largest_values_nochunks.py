import pandas as pd
import heapq
import sys
import logging
import argparse
import tempfile
import os
from datetime import datetime

log_file = ''
reject_file = ''

# Configure logging for errors and successful results
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Configure logging for rejected records
reject_logger = logging.getLogger('reject_logger')
reject_handler = logging.StreamHandler()  
# Change to logging.FileHandler(reject_file) to log to a file
reject_handler.setLevel(logging.ERROR)
reject_formatter = logging.Formatter('%(asctime)s - %(message)s')
reject_handler.setFormatter(reject_formatter)
reject_logger.addHandler(reject_handler)

def preprocess_file(input_file_path):
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    with open(input_file_path, 'r') as infile, temp_file:
        for line in infile:
            parts = line.split()
            if len(parts) != 2:
                reject_logger.error(f"Rejected record due to incorrect format: {line.strip()}")
                continue
            temp_file.write(' '.join(parts) + '\n')
    return temp_file.name

def preprocess_stdin():
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', newline='')
    for line in sys.stdin:
        parts = line.split()
        if len(parts) != 2:
            reject_logger.error(f"Rejected record due to incorrect format: {line.strip()}")
            continue
        temp_file.write(' '.join(parts) + '\n')
    temp_file.flush()
    return temp_file.name

def find_top_X_records(file_path, X):
    try:
        logging.info(f"Reading the entire file: {file_path}")
        
        # Read the entire CSV into a DataFrame
        df = pd.read_csv(file_path, sep=' ', header=None, names=['identifier', 'value'], dtype={'identifier': str}, engine='python')

        original_values = df['value'].copy()  # Copy original values for logging purposes
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        rejected_records = df[df['value'].isna()]
        if not rejected_records.empty:
            logging.error(f"Rejected {len(rejected_records)} records due to non-numeric value in column 'value':")
            for idx, row in rejected_records.iterrows():
                logging.error(f"Rejected record: identifier={row['identifier']}, value={original_values[idx]}")

        df = df.dropna(subset=['value'])
        df['value'] = df['value'].astype(int)

        # Initialize a min-heap to find the top X records
        min_heap = []
        
        for index, row in df.iterrows():
            value = row['value']
            identifier = row['identifier']
            if len(min_heap) < X:
                heapq.heappush(min_heap, (value, identifier))
            else:
                heapq.heappushpop(min_heap, (value, identifier))
        
        top_X_identifiers = {item[1] for item in min_heap}

        logging.info(f"Top {X} identifiers found: {top_X_identifiers}")
        return top_X_identifiers
    
    except pd.errors.ParserError as pe:
        logging.error(f"Parsing error: {pe}")
        raise ValueError(f"Invalid file format: {pe}")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise e

def main():
    logging.info("Execution log start")
    startlog = datetime.now()
    parser = argparse.ArgumentParser(description='Find top X identifiers based on their values.')
    parser.add_argument('X', type=int, help='Number of top values to retrieve')
    parser.add_argument('input_file', type=str, nargs='?', help='Path to the input file (optional, read from stdin if not provided)')

    args = parser.parse_args()

    X = args.X
    input_file = args.input_file
    
    try:
        if X <= 0:
            raise ValueError("X must be a positive integer.")
        
        if input_file:
            # Preprocess the input file
            temp_file_path = preprocess_file(input_file)
        else:
            logging.info("Reading from stdin")
            temp_file_path = preprocess_stdin()
        
        try:
            top_X_identifiers = find_top_X_records(temp_file_path, X)
        finally:
            os.remove(temp_file_path)
        
        if top_X_identifiers:
            logging.info(f"Successful: Top {X} identifiers:")
            for identifier in top_X_identifiers:
                print(identifier)
            endlog = datetime.now()
            total_processing_time = endlog - startlog
            print(f"Total processing time: {total_processing_time}")
        else:
            logging.info(f"No top {X} identifiers found.")
            
    except ValueError as ve:
        logging.error(f"Invalid value: {ve}")
    except FileNotFoundError as fnfe:
        logging.error(f"File not found: {fnfe}")
    except pd.errors.ParserError as pe:
        logging.error(f"Invalid file format: {pe}")
        print(f"Invalid file format: {pe}")
    except Exception as e:
        logging.error(f"Error occurred: {e}")

if __name__ == '__main__':
    main()
