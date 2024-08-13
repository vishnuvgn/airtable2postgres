import sync, sql, formatName
import json

M2M_MAPS = {}
BATCH_SIZE = 10000 # number of records to send to sql at a time

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

    all_values = tuple()
    batch_count  = 0
    for record in whittledRecords:
        id = record["id"]

        if id in deletedIds: # deleted records
            sql.deleteRows(sqlTable, formatName.createPrimaryKey(sqlTable), id)
            print(f"Deleted record: {record}")
        elif id in changedIds or id in addedIds:
            print(id)
            airFields = set(record["fields"].keys())
            pgTablePk = formatName.createPrimaryKey(sqlTable) # record_id val will go here
            PGCols = [pgTablePk]
            record_values = (id,)
            for field in airFields:
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
                    record_values += (val, )

                    PGField = formatName.changeName(field, True)
                    # print(f'PGField = {PGField}')
                    PGCols.append(PGField)

            all_values += (record_values,)
            batch_count += 1

            if batch_count == BATCH_SIZE:
                sql.upsertRows(sqlTable, PGCols, all_values)
                all_values = tuple()
                batch_count = 0

    if batch_count > 0:
        sql.upsertRows(sqlTable, PGCols, all_values)


    with open(f"airtableJsons/{tableName}.json", "w") as file:
        json.dump(whittledRecords, file)


def main():
    # tableName = "Members" # airtable
    tableNames = ['Members', 'Skills']#'Requirements', 'Squadrons', 'Groups', 'AppointmentType', 'Rifle Training', 'M249 Training', 'M4 Training', 'Grenade Training', 'M240 Training', 'Roles', 'Jobs', 'Vehicle Equipment', 'Maintenance Equipment', 'Communications Equipment', 'Aircraft Equipment', 'Slots', 'Skills']
    baseId = "app03GWdFHFCFlo9u"
    for tableName in tableNames:
        push(tableName, baseId)
    for junctionTables, recordMap in list(M2M_MAPS.items()):
        # tbl1Id is the upstream_id of the record in the main table
        # tbl2Ids is a list of upstream_ids of the records in the ref table
        for tbl1Id, tbl2Ids in recordMap.items():
            tbl1, tbl2 = junctionTables
            # EX: "MEMBERS", "SKILLS", "mem1", ["skill1", "skill2"] (these will obviously be real upstream ids)
            sql.populateJunctionTable(tbl1, tbl2, tbl1Id, tbl2Ids)
        
        del M2M_MAPS[junctionTables]
main()