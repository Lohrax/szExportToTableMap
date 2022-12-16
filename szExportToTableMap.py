'''
Created By: TheLohrax
Created on: 12/16/2022
Tested On Senzing Version 3.3.2

Modified Date/By/Reason/Tested on Sz Version:
|--
|--
|--

 Used to take a Senzing Export file and create two csv files
    These files can be used to loaded to tables
    File 1 is Entity Map
    File 2 is the Relationship Map


Format = transformFile(inFile,outFileLocation)
Example execution string
    python3 szExportToTableMap.py myExport.csv output/

G2Export.py command used to genarate the Sz Export file
    ./G2Export.py -F CSV -f 0 -x -o myExport.csv

Reference:
   G2Export - How to Consume Resolved Entity Data
       (https://senzing.zendesk.com/hc/en-us/articles/115004915547-G2Export-How-to-Consume-Resolved-Entity-Data)

Sudo Logic
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
'''

import sys
import time
import fileinput
import csv
import sqlite3
from sqlite3 import Error


fileExt = '.csv'
entityMapFile = "entityMap.csv"
relationshipMapFile = "relationshipMap.csv"


def dbConnection(dbFile):
    # Create a database connection to a SQLite
    try:
        conn = sqlite3.connect(dbFile)
        # print(sqlite3.version)
    except Error as e:
        print("Error! cannot create the database connection.")
        print(e)
        return None
    return conn


def runDDL(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        return None
    return 1


def runSQL(conn, sqlStr):
    # Returns the cursor
    try:
        cur = conn.cursor()
        cur.execute(sqlStr)
        return cur
    except Error as e:
        print(e)
        return None


def get_col(fin):
    dr = csv.DictReader(fin)  # comma is default delimiter
    fieldTypes = {}
    for entry in dr:
        feildslLeft = [f for f in dr.fieldnames if f not in fieldTypes.keys()]
        if not feildslLeft:
            break  # We're done
        for field in feildslLeft:
            data = entry[field]
            fieldTypes[field] = "varchar(1000)"

    return fieldTypes


def escapingGenerator(f):
    for line in f:
        yield line.encode("ascii", "xmlcharrefreplace").decode("ascii")


def csvToDb(conn, csvFile, dbFile, tblName):
    with open(csvFile, mode='r', encoding="utf8") as fin:
        # Get the column names from the file
        dt = get_col(fin)

        fin.seek(0)

        reader = csv.DictReader(fin)

        # Keep the order of the columns name just as in the CSV
        fields = reader.fieldnames
        cols = []

        # Set field and type
        for f in fields:
            cols.append("%s %s" % (f, dt[f]))

        # Generate create table statement:
        stmt = "CREATE TABLE " + tblName + " (%s)" % ",".join(cols)

        # Build table
        x = runDDL(conn, stmt)
        if x is None:
            return None

        fin.seek(0)
        reader = csv.reader(escapingGenerator(fin))

        # Generate and run insert statement:
        stmt = "INSERT INTO "
        stmt = stmt + tblName + " VALUES(%s);" % ','.join('?' * len(cols))
        cur = conn.cursor()
        cur.executemany(stmt, reader)
        conn.commit()

    return tblName


def wOpenFile(theFileName, md='w'):
    return open(theFileName, mode=md, newline='', encoding="utf-8")


def wFileWriter(fh):
    x = csv.writer(fh, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

    return x


def buildOutput(conn, outFileLocation, fileName, sqlStr):
    emOutputFile = wOpenFile(outFileLocation + fileName)
    emWriter = wFileWriter(emOutputFile)

    results = runSQL(conn, sqlStr)

    emWriter.writerow([i[0] for i in results.description])  # write headers
    emWriter.writerows(results)

    emOutputFile.close()


def transformFile(inFile, outFileLocation, dbFile=":memory:"):
    # Create a SQLite connection
    conn = dbConnection(dbFile)
    if conn is None:
        print("Error connecting to DB")
        return None

    # Build SQLite Table and load with Sz Export CSV
    tblNme1 = csvToDb(conn, inFile, dbFile, "CSV_DATA")
    if tblNme1 is None:
        print("Error when importing CSV to SQLite")
        return None

    # Build the CSV for the relationship map and output it to a file
    sqlString = "select distinct RESOLVED_ENTITY_ID as ENTITY_ID, "
    sqlString = sqlString + "RELATED_ENTITY_ID as RELATED_ENTITY_ID,"
    sqlString = sqlString + " MATCH_LEVEL as MATCH_LEVEL, MATCH_KEY"
    sqlString = sqlString + " as MATCH_KEY from CSV_DATA where "
    sqlString = sqlString + "RELATED_ENTITY_ID <> '0' and  ROWID > 1"
    buildOutput(conn, outFileLocation, relationshipMapFile, sqlString)

    # Build the CSV for the entity map and output it to a file
    sqlString = "select distinct RESOLVED_ENTITY_ID as ENTITY_ID, "
    sqlString = sqlString + "DATA_SOURCE, RECORD_ID, MATCH_KEY, "
    sqlString = sqlString + "JSON_DATA from CSV_DATA where "
    sqlString = sqlString + "RELATED_ENTITY_ID = '0'"
    buildOutput(conn, outFileLocation, entityMapFile, sqlString)

    conn.close()

# --main


inFile = sys.argv[1]
outFileLocation = sys.argv[2]

# Format = transformFile(inFile,outFileLocation)
# Example execution string
#     python3 szExportToTableMap.py DataIn/szExport.csv DataOut/

transformFile(inFile, outFileLocation)
