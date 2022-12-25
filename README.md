# node-red-contrib-duckdb

Basic node red node for [DuckDB](https://duckdb.org/docs/).


## Nodes

### Database:

config database path, such as `/tmp/duckdb.db`. Or use `:memory:` Please read offical docs [DuckDB Docs](https://duckdb.org/docs/connect)

### Duckdb SQL Node (duckdb-sql)

#### Sql Option:

There are some sql execution options.

* **msg.sql exec**: executes the sql query(ies) from input msg.sql, and does not return any result. [DuckDB exec](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+exec)
* **sql exec**: execute the sql query(ies) from code editor, and does not return any results. [DuckDB exec](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+exec)
* **sql all**: execute one sql query from code editor, and returns execution results. [DuckDB all](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+all)
* **sql each**: execute one sql query from code editor, and returns row by row. [DuckDB each](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+each)
* **PS**: execute the sql procedure statement from code editor, taken msg.params as parameters. msg.params must be an array. And does not return any results. [example](https://duckdb.org/docs/api/c/prepared)

#### Code Editor:

Input SQL queries.

### DuckDB Function Node (duckdb func)

This node prototyped a node-red data transform node which read from database, transform data and then insert into database. The drive behind of this idea is that I think for the personal use all the data platform exist I knew are too heavy and difficult to setup and use. Data linage would also be difficult to achive. 

This node provided an javascript code editor for transforming each row by function `msg.proc` and insert the processed data into database, also be able to output the result.
This node also provided a template for user. 
The template is 

```javascript
msg.beforeProc = "CREATE TABLE <table name>(...);"
msg.procQuery = "SELECT * FROM <prev table name>";
msg.proc = function(row) {
    // transform row from proc query
    // return insert to new table
    return "INSERT INTO <table name> VALUES(" + JSON.stringify(Object.values(row)).slice(1, -1).replaceAll('"', '\'') + ");";
}

msg.afterProc = "SELECT * FROM <table name> LIMIT 10;";
```

Batch input defined the batch size of the process query. The value default to 100.

`msg.beforeProc` defined a sql that will be executed before the process function. Usually it should create a table to which new data insert. This field is optional. 

`msg.procQuery` defined a sql that return data which will be processed from database. It should be a SELECT and **MUST NOT** end with `;`. The code will add the limit and offset for batch process. This field is required for get data from db. 

`msg.proc` defined a function which input is the row returned from sql defined in `msg.procQuery`. Function body should transform the data into some format and then return an INSERT query. The code running on background will handle the insert. 

`msg.afterProc` defined a query which will be executed after all the rows being processed. The result of this query will be added to msg.payload and pass to the next node(s). This field is optional. 

### DuckDB Import Node (duckdb import)

A node for importing csv or parquet file to duckdb. User can pass advanced sql import to msg.import as input. [DuckDB Import](https://duckdb.org/docs/data/overview)

#### Database:
config database path, such as `/tmp/duckdb.db`. Or use `:memory:`. Please read offical docs [duck db docs](https://duckdb.org/docs/connect)

#### Import Type:

* **csv**: load csv file from local and create table given file path and table name. [DuckDB Import CSV](https://duckdb.org/docs/data/csv)
* **parquet**: load parquet file from local and create table given file path and table name. [DuckDB Import Parquet](https://duckdb.org/docs/data/parquet)
* **msg.import**: execute the import sql get from input msg.import . [DuckDB Import SQLs](https://duckdb.org/docs/data/overview)

#### Table Name:
Input the create table name if choose csv or parquet.

#### File Path:
Input the csv or parquet file path.

### DuckDB Export Node (dukdb export)

A node for exporting csv or parquet file to duckdb. User can pass advanced sql from msg.export as input. [DuckDB Export](https://duckdb.org/docs/data/overview)

#### Database:
config database path, such as `/tmp/duckdb.db`. Or use `:memory:`. Please read offical docs [duck db docs](https://duckdb.org/docs/connect)

#### Export Type:

* **parquet**: from table export parquet file to local given file path and table name. [DuckDB Export Parquet](https://duckdb.org/docs/data/parquet)
* **msg.export**: execute the export sql get from input msg.export . [DuckDB export SQLs](https://duckdb.org/docs/data/overview)

#### Table Name:
Input the table name if choose parquet.

#### File Path:
Input the parquet file path.

## Examples

### 1. All the basic sql nodes

![example-flow](https://github.com/GuanyiLi-Craig/static-files/blob/b8d9a56b43eaf23249cd1c9e0f2c7bb448ba96ed/node-red-contrib-duckdb/duckdb_example.gif)

<details>
  <summary>example sql nodes flow</summary>
[
    {
        "id": "b90f4082584092f6",
        "type": "tab",
        "label": "Flow 2",
        "disabled": false,
        "info": "",
        "env": []
    },
    {
        "id": "c7d5b39d375ccb7b",
        "type": "inject",
        "z": "b90f4082584092f6",
        "name": "",
        "props": [
            {
                "p": "payload"
            },
            {
                "p": "topic",
                "vt": "str"
            }
        ],
        "repeat": "",
        "crontab": "",
        "once": false,
        "onceDelay": 0.1,
        "topic": "",
        "payload": "",
        "payloadType": "date",
        "x": 160,
        "y": 340,
        "wires": [
            [
                "196c2851bab1c113"
            ]
        ]
    },
    {
        "id": "f8e17a1eba95f02f",
        "type": "function",
        "z": "b90f4082584092f6",
        "name": "convert to array",
        "func": "var array = msg.payload;\nvar res = []\nvar h = Object.keys(array[0]);\nres.push(h);\nvar v = array.map(a => Object.values(a));\nres = res.concat(v);\nmsg.payload = res;\nreturn msg;",
        "outputs": 1,
        "noerr": 0,
        "initialize": "",
        "finalize": "",
        "libs": [],
        "x": 460,
        "y": 580,
        "wires": [
            [
                "c76990bb18d33038"
            ]
        ]
    },
    {
        "id": "c76990bb18d33038",
        "type": "table-viewer",
        "z": "b90f4082584092f6",
        "name": "",
        "property": "payload",
        "fieldType": "msg",
        "width": 200,
        "height": 160,
        "rows": "30",
        "active": true,
        "outputs": 0,
        "x": 690,
        "y": 340,
        "wires": []
    },
    {
        "id": "3a2645fb18edd47b",
        "type": "function",
        "z": "b90f4082584092f6",
        "name": "delete",
        "func": "msg.sql = \"delete from biostats where biostats.Name = 'Alex';\"\nreturn msg;",
        "outputs": 1,
        "noerr": 0,
        "initialize": "",
        "finalize": "",
        "libs": [],
        "x": 290,
        "y": 420,
        "wires": [
            [
                "42145b0bcd274e3e"
            ]
        ]
    },
    {
        "id": "c0e8af5319b8be88",
        "type": "function",
        "z": "b90f4082584092f6",
        "name": "param",
        "func": "msg.params = [\"Ruth\", \"Page\"];\nreturn msg;",
        "outputs": 1,
        "noerr": 0,
        "initialize": "",
        "finalize": "",
        "libs": [],
        "x": 290,
        "y": 500,
        "wires": [
            [
                "00f2a449f9a0102f"
            ]
        ]
    },
    {
        "id": "196c2851bab1c113",
        "type": "duckdb-sql",
        "z": "b90f4082584092f6",
        "mydb": "1f48e62e598e5e07",
        "sqlquery": "exec",
        "sql": "CREATE TABLE biostats AS SELECT * FROM '/tmp/biostats.csv';",
        "name": "sql exec",
        "x": 440,
        "y": 340,
        "wires": [
            [
                "3a2645fb18edd47b"
            ]
        ]
    },
    {
        "id": "42145b0bcd274e3e",
        "type": "duckdb-sql",
        "z": "b90f4082584092f6",
        "mydb": "1f48e62e598e5e07",
        "sqlquery": "msg.sql",
        "sql": "",
        "name": "msg.sql exec",
        "x": 450,
        "y": 420,
        "wires": [
            [
                "c0e8af5319b8be88"
            ]
        ]
    },
    {
        "id": "00f2a449f9a0102f",
        "type": "duckdb-sql",
        "z": "b90f4082584092f6",
        "mydb": "1f48e62e598e5e07",
        "sqlquery": "prepared",
        "sql": "delete from biostats where biostats.Name = $1 or biostats.Name = $2;",
        "name": "ps",
        "x": 430,
        "y": 500,
        "wires": [
            [
                "9c7c070e0d6efb38"
            ]
        ]
    },
    {
        "id": "9c7c070e0d6efb38",
        "type": "duckdb-sql",
        "z": "b90f4082584092f6",
        "mydb": "1f48e62e598e5e07",
        "sqlquery": "all",
        "sql": "select * from biostats;",
        "name": "sql all",
        "x": 290,
        "y": 580,
        "wires": [
            [
                "f8e17a1eba95f02f"
            ]
        ]
    },
    {
        "id": "9352b3746503ca70",
        "type": "comment",
        "z": "b90f4082584092f6",
        "name": "Load data",
        "info": "",
        "x": 440,
        "y": 300,
        "wires": []
    },
    {
        "id": "d89164b4ee752ec2",
        "type": "comment",
        "z": "b90f4082584092f6",
        "name": "select data",
        "info": "",
        "x": 280,
        "y": 640,
        "wires": []
    },
    {
        "id": "829611c93040f7ef",
        "type": "comment",
        "z": "b90f4082584092f6",
        "name": "delete by ps",
        "info": "",
        "x": 130,
        "y": 500,
        "wires": []
    },
    {
        "id": "1f48e62e598e5e07",
        "type": "duckdb",
        "db": ":memory:"
    }
]
</details>


### 2. DuckDB Function Node

![example-flow](https://github.com/GuanyiLi-Craig/static-files/blob/b8d9a56b43eaf23249cd1c9e0f2c7bb448ba96ed/node-red-contrib-duckdb/duckdb_function_example.gif)

<details>
  <summary>example function nodes flow</summary>
[
    {
        "id": "4c80175cd8069f06",
        "type": "tab",
        "label": "Flow 1",
        "disabled": false,
        "info": "",
        "env": []
    },
    {
        "id": "7a9768642dd4f58b",
        "type": "inject",
        "z": "4c80175cd8069f06",
        "name": "",
        "props": [
            {
                "p": "payload"
            },
            {
                "p": "topic",
                "vt": "str"
            }
        ],
        "repeat": "",
        "crontab": "",
        "once": false,
        "onceDelay": 0.1,
        "topic": "",
        "payload": "",
        "payloadType": "date",
        "x": 120,
        "y": 240,
        "wires": [
            [
                "be529dee6c636efe"
            ]
        ]
    },
    {
        "id": "be529dee6c636efe",
        "type": "duckdb-sql",
        "z": "4c80175cd8069f06",
        "mydb": "1530b6327e89473f",
        "sqlquery": "exec",
        "sql": "create table if not exists test (id int, rm varchar(10), name varchar(255));\ninsert into test values(1, 'test1', 'name1');\ninsert into test values(2, 'test2', 'name2');\ninsert into test values(3, 'test3', 'name3');\ninsert into test values(4, 'test4', 'name4');",
        "name": "insert",
        "x": 270,
        "y": 240,
        "wires": [
            [
                "c8fc2c56b140f72b"
            ]
        ]
    },
    {
        "id": "c8fc2c56b140f72b",
        "type": "duckdb func",
        "z": "4c80175cd8069f06",
        "name": "test proc",
        "mydb": "1530b6327e89473f",
        "duckdbfuncbatchsize": "100",
        "duckdbfunc": "msg.beforeProc = \"CREATE TABLE IF NOT EXISTS after(id int, name varchar(255));\"\nmsg.procQuery = \"SELECT * FROM test\";\nmsg.proc = function(row) {\n    // transform row from proc query\n    delete row['rm'];\n    // return insert to new table\n    return \"INSERT INTO after VALUES(\" + JSON.stringify(Object.values(row)).slice(1, -1).replaceAll('\"', '\\'') + \");\";\n}\nmsg.afterProc = \"SELECT * FROM after LIMIT 100;\";",
        "outputs": 1,
        "noerr": 0,
        "libs": [],
        "x": 420,
        "y": 240,
        "wires": [
            [
                "6be5bdb149a437b8",
                "03775b2a03dabe7e"
            ]
        ]
    },
    {
        "id": "6be5bdb149a437b8",
        "type": "table-viewer",
        "z": "4c80175cd8069f06",
        "name": "sql output",
        "property": "payload",
        "fieldType": "msg",
        "width": 200,
        "height": 160,
        "rows": "100",
        "active": true,
        "outputs": 0,
        "x": 420,
        "y": 280,
        "wires": []
    },
    {
        "id": "03775b2a03dabe7e",
        "type": "duckdb func",
        "z": "4c80175cd8069f06",
        "name": "convert",
        "mydb": "1530b6327e89473f",
        "duckdbfuncbatchsize": 100,
        "duckdbfunc": "msg.beforeProc = \"CREATE TABLE proc(nodeId varchar(32), data json);\"\nmsg.procQuery = \"SELECT * FROM after\";\nmsg.proc = function(row) {\n    // transform row from proc query\n    var nodeId = node.id;\n    var data = row;\n    // return insert to new table\n    return \"INSERT INTO proc VALUES('\" + nodeId + \"', '\" + JSON.stringify(data).replaceAll('\"', '\\\"') + \"');\";\n}\nmsg.nodeId = node.id;\nmsg.afterProc = \"SELECT * FROM proc where nodeId = '\" + node.id + \"' LIMIT 10;\";",
        "outputs": 1,
        "noerr": 0,
        "libs": [],
        "x": 660,
        "y": 240,
        "wires": [
            [
                "3e0bc2e5e53e65e2",
                "76638ad795fe7149"
            ]
        ]
    },
    {
        "id": "3e0bc2e5e53e65e2",
        "type": "table-viewer",
        "z": "4c80175cd8069f06",
        "name": "convert",
        "property": "payload",
        "fieldType": "msg",
        "width": 200,
        "height": 160,
        "rows": 10,
        "active": true,
        "outputs": 0,
        "x": 660,
        "y": 280,
        "wires": []
    },
    {
        "id": "76638ad795fe7149",
        "type": "duckdb func",
        "z": "4c80175cd8069f06",
        "name": "add random",
        "mydb": "1530b6327e89473f",
        "duckdbfuncbatchsize": 100,
        "duckdbfunc": "msg.procQuery = \"SELECT * FROM proc where nodeId = '\" + msg.nodeId + \"'\" ;\nmsg.proc = function(row) {\n    // transform row from proc query\n    var nodeId = node.id;\n    var data = JSON.parse(row['data']);\n    data['random'] = Math.random();\n    // return insert to new table\n    return \"INSERT INTO proc VALUES('\" + nodeId + \"', '\" + JSON.stringify(data).replaceAll('\"', '\\\"') + \"');\";\n}\nmsg.nodeId = node.id;\nmsg.afterProc = \"SELECT * FROM proc where nodeId = '\" + node.id + \"' LIMIT 10;\";",
        "outputs": 1,
        "noerr": 0,
        "libs": [],
        "x": 910,
        "y": 240,
        "wires": [
            [
                "b7d3459fbe6a1e27",
                "0b99e38dd6dcb94b"
            ]
        ]
    },
    {
        "id": "b7d3459fbe6a1e27",
        "type": "table-viewer",
        "z": "4c80175cd8069f06",
        "name": "",
        "property": "payload",
        "fieldType": "msg",
        "width": 200,
        "height": 160,
        "rows": 10,
        "active": true,
        "outputs": 0,
        "x": 910,
        "y": 280,
        "wires": []
    },
    {
        "id": "0b99e38dd6dcb94b",
        "type": "duckdb func",
        "z": "4c80175cd8069f06",
        "name": "to table",
        "mydb": "1530b6327e89473f",
        "duckdbfuncbatchsize": 100,
        "duckdbfunc": "msg.beforeProc = \"CREATE TABLE final(id int, name varchar(255), random float);\"\nmsg.procQuery = \"SELECT * FROM proc where nodeId = '\" + msg.nodeId + \"'\";\nmsg.proc = function(row) {\n    // transform row from proc query\n    var data = JSON.parse(row['data']);\n    \n    // return insert to new table\n    return \"INSERT INTO final VALUES(\" + JSON.stringify(Object.values(data)).slice(1, -1).replaceAll('\"', '\\'') + \");\";\n}\n\nmsg.afterProc = \"SELECT * FROM final LIMIT 10;\";",
        "outputs": 1,
        "noerr": 0,
        "libs": [],
        "x": 1120,
        "y": 240,
        "wires": [
            [
                "0194addb780c78f0"
            ]
        ]
    },
    {
        "id": "0194addb780c78f0",
        "type": "table-viewer",
        "z": "4c80175cd8069f06",
        "name": "",
        "property": "payload",
        "fieldType": "msg",
        "width": 200,
        "height": 160,
        "rows": 10,
        "active": true,
        "outputs": 0,
        "x": 1130,
        "y": 280,
        "wires": []
    },
    {
        "id": "1530b6327e89473f",
        "type": "duckdb",
        "db": ":memory:"
    }
]
</details>