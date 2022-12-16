# szExportToTableMap
Example script used to take a Senzing Export file and create two csv files (Enntity Map and Relationship Map)

# Dependencies
Use Senzing G2Export.py command to genarate an export file
    NOTE: Must be a export file format of CSV

Reference
    G2Export - How to Consume Resolved Entity Data
        (https://senzing.zendesk.com/hc/en-us/articles/115004915547-G2Export-How-to-Consume-Resolved-Entity-Data)

### Senzing Export Example 
    ./G2Export.py -F CSV -f 0 -x -o myExport.csv

### Python Module
sqlite3 python module must be installed

### Linux Example
    pip install sqlite3

# Usage
szExportToTableMap exportFileToProcess outputLocation

## Linux Example
    python3 szExportToTableMap mySenzingExportFile.csv ~/output/ 

# Sudo Logic
Read the Senzing export file (myExport.csv) into an in-memory SQLite tabel

    Select all rows that have a RELATED_ENTITY_ID = 0 and create a CSV flie of:
        File Name: szEntityMap.csv
            Column Name:
            ENTITY_ID (the grouping or cluster id for all records that
                belong to the same physical entity)
            DATA_SOURCE (The source system where the input record came from)
            RECORD_ID (The unique identifier that was provided for an
               input record)
            MATCH_KEY (The reason why this record was added to the entity)
               (NOTE: The first record added to the entity will not
               have a MATCH_KEY)

    Select all rows that have a RELATED_ENTITY_ID <> 0 output to CSV of:
        File Name: szRelationshipMap.csv
            ENTITY_ID (the left entity id of the relationship)
            RELATED_ENTITY_ID (the right entity id of the relationship)
            MATCH_LEVEL (the level of the relationship)
                2 = Possible match
                3 = Relationship
                11 = Disclosed Relationship
            MATCH_KEY (The reason why the two entities are realted)
