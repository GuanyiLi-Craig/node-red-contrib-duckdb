const { off } = require('process');

module.exports = function(RED) {
    "use strict";
    var duckdb= require('duckdb');
    var util = require("util");
    var vm = require("vm");
    var acorn = require("acorn");
    var acornWalk = require("acorn-walk");

    function getExecResult(query, con) {
        return new Promise(function(resolve, reject) {
            con.exec(query, function (err, rows) {
                if (err) {
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }

    function getAllResult(query, con) {
        return new Promise(function(resolve, reject) {
            con.all(query, function (err, rows) {
                if (err) {
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }

    function getEachResult(query, con) {
        return new Promise(function(resolve, reject) {
            con.each(query, function (err, rows) {
                if (err) {
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }

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

            var doQuery = async function(msg) {
                if (node.sqlquery == "msg.sql") {
                    if (typeof msg.sql === 'string') {
                        if (msg.sql.length > 0) {
                            try {
                                var row = await getExecResult(msg.sql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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
                            try {
                                var rows = await getAllResult(node.sql, node.mydbConfig.con);
                                msg.payload = rows;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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
                            try {
                                var row = await getExecResult(node.sql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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
                            try {
                                var row = await getEachResult(node.sql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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

    function createVMOpt(node, kind) {
        var opt = {
            filename: 'Function node'+kind+':'+node.id+(node.name?' ['+node.name+']':''),
            displayErrors: true
        };
        return opt;
    }

    function updateErrorInfo(err) {
        if (err.stack) {
            var stack = err.stack.toString();
            var m = /^([^:]+):([^:]+):(\d+).*/.exec(stack);
            if (m) {
                var line = parseInt(m[3]) -1;
                var kind = "body:";
                if (/setup/.exec(m[1])) {
                    kind = "setup:";
                }
                if (/cleanup/.exec(m[1])) {
                    kind = "cleanup:";
                }
                err.message += " ("+kind+"line "+line+")";
            }
        }
    }

    function DuckdbFuncNode(n) {
        RED.nodes.createNode(this,n);
        
        var node = this;
        node.name = n.name;
        node.mydb = n.mydb;
        node.duckdbfunc = n.duckdbfunc;
        node.duckdbfuncbatchsize = n.duckdbfuncbatchsize;
        node.outputs = n.outputs;
        node.libs = n.libs || [];

        node.mydbConfig = RED.nodes.getNode(this.mydb);

        if (RED.settings.functionExternalModules === false && node.libs.length > 0) {
            throw new Error(RED._("function.error.externalModuleNotAllowed"));
        }

        var functionText = "var results = null;"+
            "results = (async function(msg,__send__,__done__){ "+
            "var __msgid__ = msg._msgid;"+
            "var node = {"+
                "id:__node__.id,"+
                "name:__node__.name" +
            "};\n"+
                node.duckdbfunc+"\n"+
            "})(msg,__send__,__done__);";

        node.topic = n.topic;

        var sandbox = {
            console:console,
            util:util,
            Buffer:Buffer,
            Date: Date,
            RED: {
                util: RED.util
            },
            __node__: {
                id: node.id,
                name: node.name
            },
            context: {
                set: function() {
                    node.context().set.apply(node,arguments);
                },
                get: function() {
                    return node.context().get.apply(node,arguments);
                },
                keys: function() {
                    return node.context().keys.apply(node,arguments);
                },
                get global() {
                    return node.context().global;
                },
                get flow() {
                    return node.context().flow;
                }
            }
        };

        const moduleLoadPromises = [];

        if (node.hasOwnProperty("libs")) {
            let moduleErrors = false;
            var modules = node.libs;
            modules.forEach(module => {
                var vname = module.hasOwnProperty("var") ? module.var : null;
                if (vname && (vname !== "")) {
                    if (sandbox.hasOwnProperty(vname) || vname === 'node') {
                        node.error(RED._("function.error.moduleNameError",{name:vname}))
                        moduleErrors = true;
                        return;
                    }
                    sandbox[vname] = null;
                    var spec = module.module;
                    if (spec && (spec !== "")) {
                        moduleLoadPromises.push(RED.import(module.module).then(lib => {
                            sandbox[vname] = lib.default;
                        }).catch(err => {
                            node.error(RED._("function.error.moduleLoadError",{module:module.spec, error:err.toString()}))
                            throw err;
                        }));
                    }
                }
            });
            if (moduleErrors) {
               throw new Error(RED._("function.error.externalModuleLoadError"));
           }
        }

        var processMessage = (() => {});

        node.on("input", function(msg,send,done) {
            processMessage(msg, send, done);
        });

        Promise.all(moduleLoadPromises).then(() => {
            var context = vm.createContext(sandbox);
            try {
                node.script = vm.createScript(functionText, createVMOpt(node, ""));
                processMessage = async function (msg, send, done) {
                    context.msg = msg;
                    context.__send__ = send;
                    context.__done__ = done;
                    node.script.runInContext(context);

                    var inputMsg = context.msg;
                    var batchSize = parseInt(node.duckdbfuncbatchsize);

                    try {
                        if (typeof inputMsg.beforeProc === 'string') {
                            await getExecResult(inputMsg.beforeProc, node.mydbConfig.con);
                        }

                        if (typeof inputMsg.procQuery === 'string') {
                            var offset = 0;
                            do {
                                var batchSQLQuery = inputMsg.procQuery + " LIMIT " + batchSize.toString() + " OFFSET " + offset.toString() + ";";
                                var rows = await getAllResult(batchSQLQuery, node.mydbConfig.con);
                                var batchResQuery = "";
                                rows.forEach(async row => {
                                    batchResQuery = batchResQuery + inputMsg.proc(row) + '\n';
                                });
                                await getExecResult(batchResQuery, node.mydbConfig.con);
                                offset = offset + batchSize;
                            } while (rows.length == batchSize)
                        }

                        if (typeof inputMsg.afterProc === 'string') {
                            var response = await getAllResult(inputMsg.afterProc, node.mydbConfig.con);
                            msg.payload = response;
                        }
                        msg.beforeProc = null;
                        msg.afterProc = null;
                        msg.procQuery = null;
                        msg.proc = null;
                        node.send(msg);
                    } catch(err) {
                        node.error(err, msg);
                        done(err);
                        return;
                    }
                }

                node.on("close", function() {
                    done();
                });
            }
            catch(err) {
                updateErrorInfo(err);
                node.error(err);
            }
        }).catch(err => {
            node.error(RED._("function.error.externalModuleLoadError"));
        });
    }
    RED.nodes.registerType("duckdb func", DuckdbFuncNode, {
        dynamicModuleList: "libs",
        settings: {
            functionExternalModules: { value: true, exportable: true }
        }
    });


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

            var doImport = async function(msg) {
                if (node.duckdbimport == "msg.import") {
                    if (typeof msg.import === 'string') {
                        try {
                            var row = await getAllResult(msg.import, node.mydbConfig.con);
                            msg.payload = row;
                            node.send(msg);
                        } catch(err) {
                            node.error(err, msg);
                        }
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
                            try {
                                var row = await getAllResult(csvImportSql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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
                            try {
                                var row = await getAllResult(parquetImportSql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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

            var doExport = async function(msg) {
                if (node.duckdbexport == "msg.export") {
                    if (typeof msg.export === 'string') {
                        try {
                            var row = await getAllResult(msg.export, node.mydbConfig.con);
                            msg.payload = row;
                            node.send(msg);
                        } catch(err) {
                            node.error(err, msg);
                        }                        
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
                            try {
                                var row = await getAllResult(parquetExportSql, node.mydbConfig.con);
                                msg.payload = row;
                                node.send(msg);
                            } catch(err) {
                                node.error(err, msg);
                            }
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