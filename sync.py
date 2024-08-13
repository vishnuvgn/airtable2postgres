import requests, os
from dotenv import load_dotenv
load_dotenv()

def getRecords(baseId, tableIdOrName):
    url = f"https://api.airtable.com/v0/{baseId}/{tableIdOrName}"
    headers = {
        "Authorization": "Bearer pat6mSt609F0ELwEr.fa65ea019588625a6204ceaed6c22f0d8d5add6517f1798ec963a7dd9e9d40ef"
    }

    count = 0
    offset = ""
    totalRecords = []
    while True:
        print(count)
        if count == 0:
            response = requests.get(url, headers=headers)
        else:
            response = requests.get(url, headers=headers, params={"offset": offset})

        data = response.json()
        records = data["records"]
        for record in records:
            totalRecords.append(record)
        if "offset" not in data:
            break
        offset = data["offset"]
        count += 1
    return totalRecords
    
def whittle(syncFields, totalRecords):
    whittledRecords = []
    for record in totalRecords:
        fields = record["fields"] # fields  is a dictionary
        whittledRecord = {}
        whittledRecord["id"] = record["id"]
        whittledRecord["fields"] = {}
        for sfield in syncFields:
            if sfield not in fields:
                whittledRecord["fields"][sfield] = None
            else:
                whittledRecord["fields"][sfield] = fields[sfield]
            
        
        whittledRecords.append(whittledRecord)
    return whittledRecords

def findChanges(oldRecords, newRecords):
    oldIds = {record["id"] for record in oldRecords}
    newIds = {record["id"] for record in newRecords}

    deletedIds = oldIds - newIds
    addedIds = newIds - oldIds

    remainingIds = oldIds & newIds
    

    changedIds = set()
    for id in remainingIds:
        matching_record_old = next((record for record in oldRecords if record["id"] == id), None)
        matching_record_new = next((record for record in newRecords if record["id"] == id), None)
        if matching_record_old != matching_record_new:
            changedIds.add(id)
    print(deletedIds, addedIds, changedIds)
    return deletedIds, addedIds, changedIds




    
