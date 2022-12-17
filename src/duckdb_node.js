module.exports = function(RED) {
    "use strict";
    var duckdb= require('duckdb');

    function DuckDBNode(n) {
        RED.nodes.createNode(this,n);

        this.dbname = n.db;
        var node = this;

        node.doConnect = function() {
            if (node.db) { return; }
            node.db = new duckdb.Database(node.dbname);
            if (node.con) { return; }
            node.con = node.db.connect();
        }

        node.on('close', function (done) {
            if (node.tick) { clearTimeout(node.tick); }
            if (node.con) { node.con.close(done()); }
            if (node.db) { node.db.close(done()); }
            else { done(); }
        });
    }
    RED.nodes.registerType("duckdb",DuckDBNode);


    function DuckDBNodeSql(n) {
        RED.nodes.createNode(this,n);
        this.mydb = n.mydb;
        this.sqlquery = n.sqlquery||"msg.sql";
        this.sql = n.sql;
        this.mydbConfig = RED.nodes.getNode(this.mydb);
        var node = this;
        node.status({});

        if (node.mydbConfig) {
            node.mydbConfig.doConnect();
            node.status({fill:"green",shape:"dot",text:this.mydbConfig.dbname});

            var doQuery = function(msg) {
                if (node.sqlquery == "msg.sql") {
                    if (typeof msg.sql === 'string') {
                        if (msg.sql.length > 0) {
                            node.mydbConfig.con.exec(msg.sql, function(err, row) {
                                if (err) { node.error(err,msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        node.error("msg.sql : the query is not defined as a string",msg);
                        node.status({fill:"red",shape:"dot",text:"msg.sql error"});
                    }
                }
                if (node.sqlquery == "all") {
                    if (typeof node.sql === 'string') {
                        if (node.sql.length > 0) {
                            node.mydbConfig.con.all(node.sql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        if (node.sql === null || node.sql == "") {
                            node.error("SQL statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"SQL config not set up"});
                        }
                    }
                }
                if (node.sqlquery == "exec") {
                    if (typeof node.sql === 'string') {
                        if (node.sql.length > 0) {
                            node.mydbConfig.con.exec(node.sql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        if (node.sql === null || node.sql == "") {
                            node.error("SQL statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"SQL config not set up"});
                        }
                    }
                }
                if (node.sqlquery == "each") {
                    if (typeof node.sql === 'string') {
                        if (node.sql.length > 0) {
                            node.mydbConfig.con.each(node.sql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        if (node.sql === null || node.sql == "") {
                            node.error("SQL statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"SQL config not set up"});
                        }
                    }
                }
                if (node.sqlquery == "prepared") {
                    if (typeof node.sql === 'string' && typeof msg.params !== "undefined" && typeof msg.params === "object") {
                        if (node.sql.length > 0) {
                            node.mydbConfig.con.prepare(node.sql, function(err, stmt) {
                                stmt.all(...msg.params, function(err, row) {
                                    if (err) { node.error(err,msg); }
                                    else {
                                        msg.payload = row;
                                        node.send(msg);
                                    }
                                });
                            });
                        }
                    }
                    else {
                        if (node.sql === null || node.sql == "") {
                            node.error("Prepared statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"Prepared statement not set up"});
                        }
                        if (typeof msg.params == "undefined") {
                            node.error("msg.params not passed");
                            node.status({fill:"red",shape:"dot",text:"msg.params not defined"});
                        }
                        else if (typeof msg.params != "object") {
                            node.error("msg.params not an object");
                            node.status({fill:"red",shape:"dot",text:"msg.params not an object"});
                        }
                    }
                }
            }

            node.on("input", function(msg) {
                if (msg.hasOwnProperty("extension")) {
                    node.mydbConfig.db.loadExtension(msg.extension, function(err) {
                        if (err) { node.error(err,msg); }
                        else { doQuery(msg); }
                    });
                }
                else { doQuery(msg); }
            });
        }
        else {
            node.error("DuckDB database not configured");
        }
    }
    RED.nodes.registerType("duckdb-sql", DuckDBNodeSql);

    function DuckDBImport(n) {
        RED.nodes.createNode(this,n);

        this.mydb = n.mydb;
        this.duckdbimport = n.duckdbimport||"msg.import";
        this.tablename = n.importtablename;
        this.duckdbfile = n.duckdbimportfile;
        this.mydbConfig = RED.nodes.getNode(this.mydb);
        var node = this;
        
        if (node.mydbConfig) {
            node.mydbConfig.doConnect();
            node.status({fill:"green",shape:"dot",text:this.mydbConfig.dbname});

            var doImport = function(msg) {
                if (node.duckdbimport == "msg.import") {
                    if (typeof msg.import === 'string') {
                        node.mydbConfig.con.all(msg.import, function(err, row) {
                            if (err) { node.error(err,msg); }
                            else {
                                msg.payload = row;
                                node.send(msg);
                            }
                        });
                    }
                    else {
                        node.error("msg.import : the query is not defined as a string",msg);
                        node.status({fill:"red",shape:"dot",text:"msg.sql error"});
                    }
                }
                if (node.duckdbimport == "import-csv") {
                    if (typeof node.duckdbfile === 'string' && typeof node.tablename === 'string') {
                        if (node.duckdbfile.length > 0 && node.tablename.length > 0) {
                            var csvImportSql = "CREATE TABLE " + node.tablename + " AS SELECT * FROM '" + node.duckdbfile + "';";
                            node.mydbConfig.con.all(csvImportSql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        node.error("SQL csv import config not set up",msg);
                        node.status({fill:"red",shape:"dot",text:"SQL import config not set up"});
                    }
                }
                if (node.duckdbimport == "import-parquet") {
                    if (typeof node.duckdbfile === 'string' && typeof node.tablename === 'string') {
                        if (node.duckdbfile.length > 0 && node.tablename.length > 0) {
                            var parquetImportSql = "CREATE TABLE " + node.tablename + " AS SELECT * FROM read_parquet('" + node.duckdbfile + "');";
                            node.mydbConfig.con.all(parquetImportSql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        node.error("SQL parquet import config not set up",msg);
                        node.status({fill:"red",shape:"dot",text:"SQL import config not set up"});
                    }
                }
            }

            node.on("input", function(msg) {
                if (msg.hasOwnProperty("extension")) {
                    node.mydbConfig.db.loadExtension(msg.extension, function(err) {
                        if (err) { node.error(err,msg); }
                        else { doImport(msg); }
                    });
                }
                else { doImport(msg); }
            });
        }
        else {
            node.error("DuckDB database not configured");
        }
    }
    RED.nodes.registerType("duckdb import",DuckDBImport);

    function DuckDBExport(n) {
        RED.nodes.createNode(this,n);

        this.mydb = n.mydb;
        this.duckdbexport= n.duckdbexport||"msg.export";
        this.tablename = n.exporttablename;
        this.duckdbfile = n.duckdbexportfile;
        this.mydbConfig = RED.nodes.getNode(this.mydb);
        var node = this;
        
        if (node.mydbConfig) {
            node.mydbConfig.doConnect();
            node.status({fill:"green",shape:"dot",text:this.mydbConfig.dbname});

            var doExport = function(msg) {
                if (node.duckdbexport == "msg.export") {
                    if (typeof msg.export === 'string') {
                        node.mydbConfig.con.all(msg.export, function(err, row) {
                            if (err) { node.error(err,msg); }
                            else {
                                msg.payload = row;
                                node.send(msg);
                            }
                        });
                    }
                    else {
                        node.error("msg.export : the query is not defined as a string",msg);
                        node.status({fill:"red",shape:"dot",text:"msg.sql error"});
                    }
                }
                if (node.duckdbexport == "export-parquet") {
                    if (typeof node.duckdbfile === 'string' && typeof node.tablename === 'string') {
                        if (node.duckdbfile.length > 0 && node.tablename.length > 0) {
                            var parquetExportSql = "COPY (SELECT * FROM " + node.tablename + ") TO '" + node.duckdbfile + "' (FORMAT 'parquet');";
                            node.mydbConfig.con.all(parquetExportSql, function(err, row) {
                                if (err) { node.error(err, msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        node.error("SQL parquet import config not set up",msg);
                        node.status({fill:"red",shape:"dot",text:"SQL import config not set up"});
                    }
                }
            }

            node.on("input", function(msg) {
                if (msg.hasOwnProperty("extension")) {
                    node.mydbConfig.db.loadExtension(msg.extension, function(err) {
                        if (err) { node.error(err,msg); }
                        else { doExport(msg); }
                    });
                }
                else { doExport(msg); }
            });
        }
        else {
            node.error("DuckDB database not configured");
        }
    }
    RED.nodes.registerType("duckdb export",DuckDBExport);
}