import sqlparse
import csv
import sys
import itertools
import re
from copy import deepcopy

data = dict()
select_cols = list()
query_tables = list()
where_conds = list()
where_op = ""
distinct_flag = False
aggregate_flag = False

def load_data():
    with open('metadata.txt') as f:
        line = f.readline().strip()
        while line:
            if line == "<begin_table>":
                line = f.readline().strip()
                table = line
                data[table] = {"columns":list(),"data":list()}
                line = f.readline().strip()
                while line!="<end_table>":
                    column = line
                    data[table]["columns"].append(column.lower())
                    line = f.readline().strip()
                line = f.readline().strip()
    for table_name, table_data in data.items():
        with open(table_name + '.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                row = [x.strip() for x in row]
                row = [x.strip("'") for x in row]
                row = [int(x.strip('"')) for x in row]
                table_data["data"].append(row)

def query_parse(query):
    lines = query.split('\n')
    lines = [line.strip(',') for line in lines]
    i=0
    while i < len(lines):
        line = lines[i]
        if 'select' in line:
            select_cols.append(line.strip("select").strip())
            i += 1
            while i < len(lines) and lines[i][0] == '\t':
                select_cols.append(lines[i].strip('\t').strip())
                i += 1
        
        elif 'from' in line:
            query_tables.append(line.strip("from").strip())
            i += 1
            while i < len(lines) and lines[i][0] == '\t':
                query_tables.append(lines[i].strip('\t').strip())
                i += 1

        elif 'where' in line:
            where_conds.append(line.strip("where").strip())
            i += 1
            if i < len(lines) and lines[i][0] == '\t':
                cond2 = lines[i].strip('\t').strip().split(" ")
                operator = cond2[0]
                cond2 = ' '.join(cond2[1:])
                where_conds.append(cond2.strip())
                global where_op
                where_op = operator.strip()
            i += 1

        else:
            i += 1

def check_table_exist():
    for table in query_tables:
        if table not in list(data.keys()):
            print("Table " + table + " does not exist in the database")
            exit(0)

def flatten_cart_product(a):
    for i in range(len(a)):
        a[i] = a[i][0] + a[i][1]
    return a

def product():
    table = {"columns":list(),"data":list()}
    for table_name in query_tables:
        columns = data[table_name]["columns"]
        for i in range(len(columns)):
            columns[i] = table_name + '.' + columns[i]
        table['columns'] += columns
    
    if len(query_tables)==1:
        table["data"] = data[query_tables[0]]["data"]
        return table

    prod_mat = flatten_cart_product(list(itertools.product(data[query_tables[0]]["data"],data[query_tables[1]]["data"])))
    for i in range(2,len(query_tables)):
        prod_mat = flatten_cart_product(list(itertools.product(prod_mat,data[query_tables[i]]["data"])))
    table["data"] = prod_mat
    return table

def select(table):
    if len(where_conds) == 0:
        return table, ""
    col_list = []
    for col in table['columns']:
        col_list.append(col.split('.')[1])
    print(where_conds)
    col_indices = []
    for i, cond  in enumerate(where_conds):
        for j in range(2):
            col_name = re.split('=|<=|>=|<|>', cond)[j]
            if col_list.count(col_name.strip()) >= 2:
                print("Ambiguous column name " + col_name.strip())
                exit(0)
            if col_name.strip() in col_list:
                idx = col_list.index(col_name.strip())
                col_indices.append(idx)
                c = table["columns"][idx]
                cond = cond.replace(col_name,c)
            elif col_name.strip() in table['columns']:
                idx = table['columns'].index(col_name.strip())
                col_indices.append(idx)
            else:
                try:
                    int(col_name.strip())
                except:
                    print("Column " + col_name.strip() + " does not exist")
                    exit(0)
        if '=' in cond and cond[cond.index('=')-1]!='<' and cond[cond.index('=')-1]!='>':
            cond = cond.replace('=','==')

        where_conds[i] = cond

    eval_string = where_conds[0] + ' '
    for cond in where_conds[1:]:
        eval_string += where_op + ' ' + cond + ' '

    data = deepcopy(table['data'])
    for row in data:
        e = eval_string
        for idx in col_indices:
            e = e.replace(table["columns"][idx],str(row[idx]))
        if not eval(e):
            table['data'].remove(row)
    return table, eval_string

def drop_duplicate(table, eval_string):
    if '==' in eval_string and eval_string.count('.')==2:
        drop_idx = table['columns'].index(eval_string.split("==")[1].strip())
        for row in table['data']:
            del(row[drop_idx])
        del(table["columns"][drop_idx])
    return table

def project(table, eval_string):
    for c in select_cols:
        if '*' in c:
            if where_op:
                return drop_duplicate(drop_duplicate(table, eval_string.split(where_op)[0]), eval_string.split(where_op)[1])
            else:
                return drop_duplicate(table, eval_string)
    col_list = []
    for col in table['columns']:
        col_list.append(col.split('.')[1])
    temp_cols = []
    temp_col_idx = []
    for c in select_cols:
        if 'max' in c or 'min' in c or 'sum' in c or 'average' in c:
            c = c.split('(')[1].strip(')').strip()
            global aggregate_flag
            aggregate_flag = True
        if 'distinct' in c:
            c = c.split('distinct')[1].strip()
            global distinct_flag
            distinct_flag = True
        if c not in col_list and c not in table['columns']:
            print("Column " + c + " does not exist")
            exit(0) 
        if '.' not in c:
            if col_list.count(c) >= 2:
                print("Ambiguous column name " + c)
                exit(0)
            c = table['columns'][col_list.index(c)]
        temp_cols.append(c)
        temp_col_idx.append(table['columns'].index(c))
    for i,row in enumerate(table['data']):
        table['data'][i] = [row[j] for j in temp_col_idx]
    table['columns'] = [table['columns'][j] for j in temp_col_idx]
    return table

def distinct(table):
    grouped = itertools.groupby(table['data'])
    groups = list(k for k,_ in grouped)
    table['data'] = groups
    return table

def aggregate(table):
    col_names = []
    for col in table['columns']:
        col_names.append(col.split('.')[1])

    col_idx = []
    eval_strings = []
    col_titles = []
    for c in select_cols:
        c = c.split('(')
        func = c[0].strip()
        c = c[1].strip(')').strip()
        if '.' not in c:
            c = table['columns'][col_names.index(c)]
        if func=='average':
            eval_strings.append('sum(' + c + ')/len(' + c + ')')
        else:
            eval_strings.append(func + '(' + c + ')')
        col_titles.append(func + '(' + c + ')')
        col_idx.append(table['columns'].index(c))
    ret_table = {"columns":col_titles, "data":[]}
    for i,idx in enumerate(col_idx):
        col_values = [row[idx] for row in table['data']]
        eval_strings[i] = eval_strings[i].replace(table['columns'][idx],str(col_values))
    for e in eval_strings:
        if len(eval(e.split('(')[1].strip(')'))) == 0:
            ret_table["data"].append('')
        else:   
            ret_table["data"].append(eval(e))
    ret_table["data"] = [ret_table["data"]]
    return ret_table

def display(table):
    print(','.join(table['columns']))
    for row in table['data']:
        print(','.join([str(val) for val in row]))

if __name__ == '__main__':
    load_data()              
    data = {k.lower(): v for k, v in data.items()}
    query = sys.argv[1]
    if query[-1]!=';':
        print("Command must be terminated by a semicolon")
        exit(0)
    try:
        query = query.strip(';')
        indented_query = sqlparse.format(query,reindent=True, indent_tabs=True, keyword_case='lower', identifier_case='lower')
        query_parse(indented_query)
        check_table_exist()
        output_table = product()
        output_table, eval_string = select(output_table)
        output_table = project(output_table, eval_string)
        if distinct_flag:
            output_table = distinct(output_table)
        if aggregate_flag:
            output_table = aggregate(output_table)
        display(output_table)
    except:
        print("Wrong query format")
