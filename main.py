import sync, sql, formatName
import json

M2M_MAPS = {}


def push(tableName, baseId):
    
    totalRecords = sync.getRecords(baseId, tableName)
    syncFields = set(json.load(open("AirTableFields.json"))[tableName])
    whittledRecords = sync.whittle(syncFields, totalRecords)
    
    
    try:
        with open(f"airtableJsons/{tableName}.json", "r") as file:
            oldRecords = json.load(file)
    except FileNotFoundError:
        oldRecords = []


    deletedIds, addedIds, changedIds = sync.findChanges(oldRecords, whittledRecords)
    
    
    airtable2sqlMap = json.load(open("AirtablePGTableMap.json"))
    sqlTable = airtable2sqlMap[tableName]


    for record in whittledRecords:
        id = record["id"]

        if id in deletedIds: # deleted records
            sql.deleteRows(sqlTable, formatName.createPrimaryKey(sqlTable), id)
            print(f"Deleted record: {record}")
        elif id in changedIds or id in addedIds:
        # else: # added or mutated records
            airFieldsSent = set(record["fields"].keys())
            airFieldsNotSent = syncFields - airFieldsSent
            pgTablePk = formatName.createPrimaryKey(sqlTable) # record_id val will go here
            PGCols = [pgTablePk]
            values = (id,)
            for field in airFieldsSent:
                val = record["fields"][field]
                
                M2M_flag = False

                if field[-3:] == "M2M": # value is a list
                    M2M_flag = True
                    print(f'found M2M: {field}')
                    refTable = field[:-4] # has to be spelled right in airtable
                    junctionTables = (sqlTable, formatName.changeName(refTable, False)) # tuple is key in dict
                    if junctionTables not in M2M_MAPS:
                        M2M_MAPS[junctionTables] = {
                            id : val # will be list of rec ids for the ref table
                        }
                    else:
                        M2M_MAPS[junctionTables][id] = val

                    print(val)

                if type(val) == list and len(val) == 1:
                    val = val[0]
                # elif type(val) == int or type(val) == float:
                #     val = str(val)
                elif type(val) == dict:
                    with open("error.txt", "w") as f:
                        f.write(f'error {field}: {val}')
                    val = ""
                elif val == None:
                    val = ""

                
                if M2M_flag == False: # M2M not a field in pg sql table. will be handled in the junction table which can only be populated after all tables populated
                    values += (val, )

                    PGField = formatName.changeName(field, True)
                    # print(f'PGField = {PGField}')
                    PGCols.append(PGField)


            for field in airFieldsNotSent:
                M2M_flag = False
                if field[-3:] == "M2M": # value is a list
                    M2M_flag = True
                    print(f'found M2M: {field}')
                    refTable = field[:-4] # has to be spelled right in airtable
                    junctionTables = (sqlTable, formatName.changeName(refTable, False)) # tuple is key in dict
                    if junctionTables not in M2M_MAPS:
                        M2M_MAPS[junctionTables] = {
                            id : [] # blank bc it has been deleted
                        }
                    else:
                        M2M_MAPS[junctionTables][id] = [] # blank bc it has been deleted

                
                if M2M_flag == False: # M2M not a field in pg sql table. will be handled in the junction table which can only be populated after all tables populated
                    values += ("", )

                    PGField = formatName.changeName(field, True)
                    # print(f'PGField = {PGField}')
                    PGCols.append(PGField)
            
            sql.upsertRow(sqlTable, PGCols, values)


    with open(f"airtableJsons/{tableName}.json", "w") as file:
        json.dump(whittledRecords, file)

def main():
    tableName = "Members" # airtable
    baseId = "app03GWdFHFCFlo9u"
    push(tableName, baseId)
    # for junctionTables, recordMap in list(M2M_MAPS.items()):
    #     # tbl1Id is the upstream_id of the record in the main table
    #     # tbl2Ids is a list of upstream_ids of the records in the ref table
    #     for tbl1Id, tbl2Ids in recordMap.items():
    #         tbl1, tbl2 = junctionTables
    #         # EX: "MEMBERS", "SKILLS", "mem1", ["skill1", "skill2"] (these will obviously be real upstream ids)
    #         sql.populateJunctionTable(tbl1, tbl2, tbl1Id, tbl2Ids)
        
    #     del M2M_MAPS[junctionTables]
main()