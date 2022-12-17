# node-red-contrib-duckdb

Basic node red node for [DuckDB](https://duckdb.org/docs/).

![example-flow](https://github.com/GuanyiLi-Craig/static-files/blob/b8d9a56b43eaf23249cd1c9e0f2c7bb448ba96ed/node-red-contrib-duckdb/duckdb_example.gif)

<details>
  <summary>example flow</summary>
```json
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
```
</details>

## Config

![duckdb-sql-config](https://github.com/GuanyiLi-Craig/static-files/blob/b8d9a56b43eaf23249cd1c9e0f2c7bb448ba96ed/node-red-contrib-duckdb/duckdb_sql_config.png)

#### Database:

config database path, such as `/tmp/duckdb.db`. Or use `:memory:` Please read offical docs [DuckDB Docs](https://duckdb.org/docs/connect)

#### Sql Option:

There are 4 options.

* **msg.sql exec**: executes the sql query(ies) from input msg.sql, and does not return any result. [DuckDB exec](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+exec)
* **sql exec**: execute the sql query(ies) from code editor, and does not return any results. [DuckDB exec](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+exec)
* **sql all**: execute one sql query from code editor, and returns execution results. [DuckDB all](https://duckdb.org/docs/api/nodejs/reference#module_duckdb..Connection+all)
* **PS**: execute the sql procedure statement from code editor, taken msg.params as parameters. msg.params must be an array. And does not return any results. [example](https://duckdb.org/docs/api/c/prepared)

#### Code Editor:

Input SQL queries.
