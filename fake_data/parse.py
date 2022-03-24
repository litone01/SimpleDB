from msilib import schema
import sys
from pathlib import Path

# dataset_folder_names = ["100", "1000", "5000", "10000", "15000"]
dataset_folder_names = ["50"]
table_schemas = {
    "STUDENT": [("sid", "int"), ("sname", "varchar(50)")],
    "STAFF": [("stid", "int"), ("stname", "varchar(50)")],
    "COURSE": [("cid", "int"), ("cname", "varchar(50)")],
    "SECTION": [("secid", "int"), ("courseid", "int"), ("staffid", "int")],
    "ENROLL": [("eid", "int"), ("studentid", "int"), ("sectionid", "int")]
}

def updateCreateQuery(table_name):
    current_schema = table_schemas[table_name]
    result = f"CREATE TABLE {table_name} ("
    for col_name, col_type in current_schema:
        result += f"{col_name} {col_type}, "
    result = result[:-2] + ")"
    return result

# currently, row index is '1', need to convert to 1 before inserting
# eg. ('1', 'a', 'b', 'c') -> (1, 'a', 'b', 'c')
# we can assume that the first column is always the id
def convertIndexToInt(query):
    queries = query.split(",", 1)
    row_index = queries[0][1:].replace("'", "")
    return f"({row_index},{queries[1]}"

def updateInsertQuery(query, table_name):
    current_schema = table_schemas[table_name]
    index = query.find("VALUES")
    if index == -1:
        print("Error: query does not contain VALUES")
        sys.exit(1)
    # -1 is used to remove the trailing ';' 
    query_after_values = f"VALUES{convertIndexToInt(query[index+6:-1])}"
    query_before_values = f"INSERT INTO {table_name} ("
    for col_name, _ in current_schema:
        query_before_values += f"{col_name}, "
    query_before_values = query_before_values[:-2] + ") "
    return query_before_values + query_after_values

def parse(query_file):
    query_file = Path(query_file)
    table_name = query_file.stem
    if not query_file.exists():
        print("Error: query file does not exist")
        sys.exit(1)

    # read file content
    lines = query_file.read_text().splitlines()
    updated_lines = []
    curr_line_num = 1
    for line in lines:
        # skip first line, which is comment
        if curr_line_num == 1:
            pass
        elif curr_line_num == 2:
            updated_lines.append(updateCreateQuery(table_name))
        elif len(line) == 0:
            pass
        else:
            updated_lines.append(updateInsertQuery(line, table_name))
        curr_line_num += 1


    # write to file and replace the cntent
    query_file.write_text('\n'.join(updated_lines))
    print(f"Successfully updated query file {query_file}, {len(updated_lines)} lines updated")

def main(root_folder_path):
    for dataset_folder_name in dataset_folder_names:
        dataset_folder_path = Path(root_folder_path) / dataset_folder_name
        for file_name in table_schemas.keys():
            query_file_path = dataset_folder_path / f"{file_name}.sql"
            parse(query_file_path)

if __name__ == '__main__':
    # python3 parse.py <absolute_path_of_root_dataset_folder>
    main("C:\\Users\\Jiaxiang\\git\\cs3223-project\\fake_data")