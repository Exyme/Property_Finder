#!/bin/bash

# Property Finder Automation Script
# This script runs the property finder with the correct Python environment

# Set the working directory
cd /Users/isuruwarakagoda/Projects/Property_Finder

# Set up logging
LOG_FILE="/Users/isuruwarakagoda/Projects/Property_Finder/output/launchagent.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Log start time
echo "[$TIMESTAMP] Starting Property Finder automation..." >> "$LOG_FILE"

# Run the script and capture output
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 property_finder.py >> "$LOG_FILE" 2>&1

# Log completion
EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$TIMESTAMP] Property Finder completed successfully (exit code: $EXIT_CODE)" >> "$LOG_FILE"
else
    echo "[$TIMESTAMP] Property Finder completed with errors (exit code: $EXIT_CODE)" >> "$LOG_FILE"
fi
