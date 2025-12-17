#!/bin/bash

# Property Finder Automation Script
# This script runs the property finder with the correct Python environment

# Set the working directory to the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Set up logging
LOG_FILE="$SCRIPT_DIR/output/launchagent.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Log start time
echo "[$TIMESTAMP] Starting Property Finder automation..." >> "$LOG_FILE"

# Run the script and capture output
# Note: Adjust the Python path to match your environment
python3 property_finder.py >> "$LOG_FILE" 2>&1

# Log completion
EXIT_CODE=$?
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$TIMESTAMP] Property Finder completed successfully (exit code: $EXIT_CODE)" >> "$LOG_FILE"
else
    echo "[$TIMESTAMP] Property Finder completed with errors (exit code: $EXIT_CODE)" >> "$LOG_FILE"
fi
