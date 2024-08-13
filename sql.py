import psycopg2
from dotenv import load_dotenv
import os, json, sys
import formatName, jsonFunctions, airtables # i dont like this structure all that much...

load_dotenv()
PG_HOST=os.getenv('PG_HOST')
PG_DATABASE=os.getenv('PG_DATABASE')
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')
PG_SCHEMA=os.getenv('PG_SCHEMA')

def mapAirtableToSQL(tables = json.load(open("AirTableFields.json"))): 
    at_pg_map = {}
    pg_table_fields = {}
    for airTable, airFields in tables.items():
        pgFields = []
        pgTable = formatName.changeName(airTable, False)
        fkCount = 0
        for field in airFields:

            pgFieldName = formatName.changeName(field, True)
            
#             # many to many relation list will NOT be put into sql table
            if pgFieldName[-3:] == "M2M".lower():
                continue
                

            pgFields.append(pgFieldName)
        

        at_pg_map[airTable] = pgTable
        pg_table_fields[pgTable] = pgFields
        
        # these two columns are needed for the upsert (sequin). 
        # if i already put them in the json, i don't have to add them again
        
        # will always be the name of the table formatted as a field + _id
        
        pgTablePk = formatName.createPrimaryKey(pgTable)
        pg_table_fields[pgTable].append(pgTablePk)


    jsonFunctions.overwrite_json("AirtablePGTableMap.json", at_pg_map) # airtable to pg table
    jsonFunctions.overwrite_json("PostgresTableFields.json", pg_table_fields)

PG_TABLE_FIELDS = json.load(open("PostgresTableFields.json"))


'''
input: string
output: string
fields and tables are surrounded by double quotes to preserve case sensitivity in pgdb
'''
def writeQuery(table, cols):
    pgTablePk = formatName.createPrimaryKey(table)
    columnsQuery = '('
    placeholders = '('
    numOfFields = len(cols)
    for i in range(numOfFields):
        col = cols[i]
        if i == numOfFields - 1: # last field
            columnsQuery += f', "{col}")'
            placeholders += '%s)'
        elif i == 0: # first field
            columnsQuery += f'"{col}"'
            placeholders += '%s, '
        else:
            columnsQuery += f', "{col}"'
            placeholders += '%s, '

    # fieldnames / excluded query
    excludedQuery = ''
    first = True

    for i in range(numOfFields):
        col = cols[i]
        if col != pgTablePk:
            if not first:
                excludedQuery += ', '
            excludedQuery += f'"{col}" = excluded."{col}"'
            first = False
            

    # Prepare the SQL query

    query = '''
    INSERT INTO "''' + table + '''" ''' + columnsQuery + '''
    VALUES ''' + placeholders + '''
    ON CONFLICT ("''' + pgTablePk + '''") DO UPDATE
    SET ''' + excludedQuery + ''';'''

    return query
    
def upsertRow(table, cols, values):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    
    query = writeQuery(table, cols)
    
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()
    
    

def createTable(table):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    query = f'CREATE TABLE IF NOT EXISTS "{table}" ('
    
    for i in range(len(PG_TABLE_FIELDS[table])):
        field = PG_TABLE_FIELDS[table][i]
        pgTablePk = formatName.createPrimaryKey(table)
        prefix = ", "
        if i == 0:
            prefix = ""
    
        # if field == "updated_idx":
        #     query += f'{prefix}"{field}" BIGINT'

        if field == pgTablePk: 
            query += f'{prefix}"{field}" TEXT PRIMARY KEY'

        else:
            query += f'{prefix}"{field}" TEXT'
    
    query += ');'
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query)
    print(f'created {table}')
    conn.commit()
    cur.close()
    conn.close()

def createJunctionTable(table1, table2):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
  
    table1_pk = formatName.createPrimaryKey(table1)
    table2_pk = formatName.createPrimaryKey(table2)

    junction_table_name = formatName.createJunctionTableName(table1, table2)

    # query still has constraints because it will be populated last anyways
    query = f'''
    CREATE TABLE IF NOT EXISTS "{junction_table_name}" (
        "{table1_pk}" TEXT,
        "{table2_pk}" TEXT,
        PRIMARY KEY ("{table1_pk}", "{table2_pk}"),
        FOREIGN KEY ("{table1_pk}") REFERENCES "{table1}"("{table1_pk}") ON DELETE CASCADE,
        FOREIGN KEY ("{table2_pk}") REFERENCES "{table2}"("{table2_pk}") ON DELETE CASCADE
    );
    '''
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query)
    print(f'created {junction_table_name}')
    conn.commit()
    cur.close()
    conn.close()

# only checking first part of pk tuple
def countRows(table, searchPk, searchId):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    query = f'''
    SELECT COUNT(*) 
    FROM "{table}"  
    WHERE "{searchPk}" = %s;
    '''
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query, (searchId,)) # has to be a single element tuple
    
    count = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    return count

def deleteRows(table, searchPk, searchId):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    
    # Define the delete query
    query = f'''
    DELETE FROM "{table}"
    WHERE "{searchPk}" = %s;
    '''
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query, (searchId,))  # has to be a single element tuple
    
    conn.commit()  
    
    cur.close()
    conn.close()


def populateJunctionTable(table1, table2, table1Id, table2Ids):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()
    junction_table_name = formatName.createJunctionTableName(table1, table2)
    table1_pk = formatName.createPrimaryKey(table1)
    table2_pk = formatName.createPrimaryKey(table2)

    deleteRows(junction_table_name, table1_pk, table1Id)

    
    for table2Id in table2Ids:
        query = f'''
        INSERT INTO "{junction_table_name}" ("{table1_pk}", "{table2_pk}")
        VALUES (%s, %s)
        ON CONFLICT ("{table1_pk}", "{table2_pk}")
        DO UPDATE SET "{table1_pk}" = EXCLUDED."{table1_pk}", "{table2_pk}" = EXCLUDED."{table2_pk}"
        '''
        
        cur.execute(f"SET search_path TO {PG_SCHEMA}")
        cur.execute(query, (table1Id, table2Id))


    print(f'populated {junction_table_name}')
    conn.commit()
    cur.close()
    conn.close()
    return

# be fucking careful
def deleteTable(table):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    query = f'DROP TABLE IF EXISTS "{table}" CASCADE'
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
    print(f'deleted {table}') 
    
def clearTable(table):
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    query = f'DELETE FROM "{table}"'
    
    cur.execute(f"SET search_path TO {PG_SCHEMA}")
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def createTables():
    tbls = list(PG_TABLE_FIELDS.keys())
    for tbl in tbls:
        createTable(tbl)

def deleteTables():
    print(f'DB: {PG_DATABASE}')
    validation = input("Are you sure this is the database you want to clear (Y/n): ")
    if validation == "y" or validation == "Y":

        tbls = list(PG_TABLE_FIELDS.keys())
        for tbl in tbls:
            deleteTable(tbl)
    else:
        return "aborted"

def clearTables():
    print(f'DB: {PG_DATABASE}')
    validation = input("Are you sure this is the database you want to clear (Y/n): ")
    if validation == "y" or validation == "Y":
        tbls = list(PG_TABLE_FIELDS.keys())
        for tbl in tbls:
            clearTable(tbl)
    else:
        return "aborted"

def restart():
    print(f'DB: {PG_DATABASE}')
    validation = input("Are you sure this is the database you want to clear (Y/n): ")
    if validation == "y" or validation == "Y":
        deleteTables()
        createTables()
    else:
        return "aborted"

# if __name__ == '__main__':
#     if len(sys.argv) < 2:
#         print("Usage: python3 script.py <function_name> [<args>...]")
#         sys.exit(1)

#     function_name = sys.argv[1]
#     function_args = sys.argv[2:]

#     if function_name in globals():
#         func = globals()[function_name]
#         # Check if the function requires arguments
#         if func.__code__.co_argcount > 0:
#             func(*function_args)  # Unpack arguments as separate positional arguments
#         else:
#             func()  # Call function without arguments
#     else:
#         print(f"Function {function_name} not found in script.")
#         sys.exit(1)