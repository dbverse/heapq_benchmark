## Objective
Use `heapq` and `chunksize` to get top N values from a dataset containing two columns: "id" (string) and "value" (integer).  
Total number of records = 1 million. Chunksize considered = 10,000.

### Observations
Three implementations were done as below:
1. **With chunksize and heapq** - Total Time taken for processing is 1 minute 13 seconds.
2. **With heapq only** - Total Time taken for processing is 1 minute 11 seconds.
3. **With Spark** - Total Time taken for processing is 11 seconds (after Spark session is created).

In Spark, the total time taken is 25 seconds, including kernel and Spark session creation time.

### Execution of .py modules in terminal
```bash
C:\Users\XXXX\heapq_benchmark\src\x_largest_values_chunks.py 2 C:/Users/XXXX/data/heapq_benchmark/test_data.txt
C:\Users\XXXX\heapq_benchmark\src\x_largest_values_nochunks.py 2 C:/Users/XXXX/data/heapq_benchmark/test_data.txt

```
### Comments
- Exception and reject record handling is done in the Python modules but not in Spark.
- In Spark implementation, invalid or incorrect datatypes are perceived as nulls and they are dropped.

