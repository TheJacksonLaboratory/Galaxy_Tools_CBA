"""
This module contains methods that access the Galaxy database.

The assumption is that it may be easier in certain situations 
to discover data from the Galaxy database than to use the API.

For documentation on sqlite3 use see:
    https://docs.python.org/3/library/sqlite3.html

"""
import sqlite3
import json
import sys

def getOriginalFilename(uuid):

    uuid = uuid[uuid.rfind('-')+1:len(uuid)]
    con = sqlite3.connect("/projects/galaxy/database/universe.sqlite")
    cur = con.cursor()
    
    queryStmt = """ 
    SELECT history_dataset_association.name
    FROM
    dataset INNER JOIN history_dataset_association ON (history_dataset_association.dataset_id = dataset.id)
    WHERE
    dataset.uuid LIKE '{0}'  
    """
    uuid = "%" + uuid + "%"
    #print(queryStmt.format(uuid))

    res = cur.execute(queryStmt.format(uuid))
    res.fetchall()

    for row in cur.execute(queryStmt.format(uuid)):
        #print(row)
        #val = row[0]
        #valLs = json.loads(val)
        #valDict = valLs[0]
        #valName = valDict["NAME"]
        valName = row[0]
        print(valName)
        return valName

def main():
    getOriginalFilename(sys.argv[1])

if __name__ == '__main__':
   main()
