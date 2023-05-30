import csv
import concurrent.futures
from collections import defaultdict
from functools import reduce

# Read passenger flight data from the CSV file
with open('AComp_Passenger_data_no_error.csv', 'r') as f:
    reader = csv.reader(f)
    passenger_data = list(reader)

# Split the data into chunks for parallel processing
def chunks(data, size):
    return [data[i:i + size] for i in range(0, len(data), size)]

# Mapper: Create a dictionary where the key is passenger_id and the value is the number of flights
def mapper(chunk):
    flight_counts = defaultdict(int)
    for row in chunk:
        passenger_id = row[0]
        flight_counts[passenger_id] += 1
    return flight_counts

# Reducer: Accumulate the number of flights for each passenger
def reducer(flight_counts1, flight_counts2):
    for passenger_id, count in flight_counts2.items():
        flight_counts1[passenger_id] += count
    return flight_counts1

# MapReduce-like implementation
# Determine the size of each chunk
chunk_size = len(passenger_data) // 5  # or choose another number based on your needs
data_chunks = chunks(passenger_data, chunk_size)


# Create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Map phase: execute the mapper function in parallel for each chunk of data
    future_to_chunk = {executor.submit(mapper, chunk): chunk for chunk in data_chunks}

    # Collect results as they become available
    mapped_data = []
    for future in concurrent.futures.as_completed(future_to_chunk):
        try:
            result = future.result()
        except Exception as exc:
            print(f'Generated an exception: {exc}')
        else:
            mapped_data.append(result)

# Reduce phase: combine all results
reduced_data = reduce(reducer, mapped_data)

# Find the passenger with the highest number of flights
passenger_id, highest_flight_count = max(reduced_data.items(), key=lambda x: x[1])

# Print the result
print(f"Passenger with the highest number of flights: {passenger_id}")
print(f"Number of flights: {highest_flight_count}")
