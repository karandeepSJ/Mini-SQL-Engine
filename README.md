# Mini-SQL-Engine
Python-based SQL Engine to execute basic SQL queries
## Usage
```
./run.sh "<query>"
```

## Example
```
./run.sh "select * from table1;"
```
```
./run.sh "select * from table1, table2;"
```
```
./run.sh "select table1.A, table2.D from table1, table2 where table1.B=table2.B and A>0;"
```
