# Parallel CSV Sorting

This program sorts large CSV files that cannot fit into memory. It uses a divide and conquer strategy to split the file into smaller chunks, sort each chunk, and then merge them.

## Prerequisites

- Java 8 or higher
- Maven

## How to Run

1. Clone the repository:
    ```bash
    git clone https://github.com/talzuzut/parallel-csv-sorting.git
    ```
2. Navigate to the project directory:
    ```bash
    cd parallel-csv-sorting
    ```
3. Build the project:
    ```bash
    mvn clean package
    ```
4. Run the program:
    ```bash
    java -jar target/parallel-csv-sorting-1.0-SNAPSHOT-jar-with-dependencies.jar <keyFieldIndex> <maxRecordsInMemory> <input_file> <output_file> <numThreads>
    ```
- `<keyFieldIndex>`: The index of the field to sort by. The first field is 0, the second is 1, etc.
- `<maxRecordsInMemory>`: The maximum number of records to keep in memory at a time.
- `<input_file>`: The path to the input CSV file.
- `<output_file>`: The path to the output CSV file.
- `<numThreads>`: The number of threads to use for sorting and merging.

## Example

```bash
java -jar target/parallel-csv-sorting-1.0-SNAPSHOT-jar-with-dependencies.jar 0 10 input.csv output.csv 2