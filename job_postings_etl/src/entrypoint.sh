#!/bin/bash

# Run any setup steps or pre-processing tasks here
echo "Running ETL to move job postings data from csvs to Neo4j..."

# Run the ETL script
python job_postings_bulk_csv_write.py
