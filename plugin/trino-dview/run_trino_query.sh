##!/bin/bash
#
#OUTPUT_FILE="/Users/apple-macbookpro/dviewProjects/trino/plugin/trino-dview/output/output.log"
#ERROR_FILE="/Users/apple-macbookpro/dviewProjects/trino/plugin/trino-dview/error/error.log"
#
#while true; do
#    # Command to run inside the container for the count query
#    COUNT_CMD='
#    trino --catalog dview << EOF
#    use blog;
#    select count(1) from dview_analytics_account_students;
#    EOF
#    '
#
#    # Run the count query inside the container and append output to output file
#    docker exec -i trino bash -c "$COUNT_CMD" >> "$OUTPUT_FILE" 2>> "$ERROR_FILE"
#
#    # Command to run inside the container for the select query
#    SELECT_CMD='
#    trino --catalog dview << EOF
#    use blog;
#    select * from dview_analytics_account_students;
#    EOF
#    '
#
#    # Run the select query inside the container and discard output
#    docker exec -i trino bash -c "$SELECT_CMD" > /dev/null 2>> "$ERROR_FILE"
#
#    # Wait for 60 seconds before running the loop again
#    sleep 5
#done
