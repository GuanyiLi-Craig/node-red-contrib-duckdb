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



    function sendResults(node,send,_msgid,msgs,cloneFirstMessage) {
        if (msgs == null) {
            return;
        } else if (!util.isArray(msgs)) {
            msgs = [msgs];
        }
        var msgCount = 0;
        for (var m=0; m<msgs.length; m++) {
            if (msgs[m]) {
                if (!util.isArray(msgs[m])) {
                    msgs[m] = [msgs[m]];
                }
                for (var n=0; n < msgs[m].length; n++) {
                    var msg = msgs[m][n];
                    if (msg !== null && msg !== undefined) {
                        if (typeof msg === 'object' && !Buffer.isBuffer(msg) && !util.isArray(msg)) {
                            if (msgCount === 0 && cloneFirstMessage !== false) {
                                msgs[m][n] = RED.util.cloneMessage(msgs[m][n]);
                                msg = msgs[m][n];
                            }
                            msg._msgid = _msgid;
                            msgCount++;
                        } else {
                            var type = typeof msg;
                            if (type === 'object') {
                                type = Buffer.isBuffer(msg)?'Buffer':(util.isArray(msg)?'Array':'Date');
                            }
                            node.error(RED._("function.error.non-message-returned",{ type: type }));
                        }
                    }
                }
            }
        }

        if (msgCount>0) {
            send(msgs);
        }
    }

    function createVMOpt(node, kind) {
        var opt = {
            filename: 'Function node'+kind+':'+node.id+(node.name?' ['+node.name+']':''), // filename for stack traces
            displayErrors: true
            // Using the following options causes node 4/6 to not include the line number
            // in the stack output. So don't use them.
            // lineOffset: -11, // line number offset to be used for stack traces
            // columnOffset: 0, // column number offset to be used for stack traces
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
                    "name:__node__.name,"+
                    "path:__node__.path,"+
                    "outputCount:__node__.outputCount,"+
                    "log:__node__.log,"+
                    "error:__node__.error,"+
                    "warn:__node__.warn,"+
                    "debug:__node__.debug,"+
                    "trace:__node__.trace,"+
                    "on:__node__.on,"+
                    "status:__node__.status,"+
                    "send:function(msgs,cloneMsg){ __node__.send(__send__,__msgid__,msgs,cloneMsg);},"+
                    "done:__done__"+
                "};\n"+
                node.duckdbfunc+"\n"+
            "})(msg,__send__,__done__);";

        var handleNodeDoneCall = true;
    
        // Check to see if the Function appears to call `node.done()`. If so,
        // we will assume it is well written and does actually call node.done().
        // Otherwise, we will call node.done() after the function returns regardless.
        if (/node\.done\s*\(\s*\)/.test(functionText)) {
            // We have spotted the code contains `node.done`. It could be in a comment
            // so need to do the extra work to parse the AST and examine it properly.
            acornWalk.simple(acorn.parse(functionText,{ecmaVersion: "latest"} ), {
                CallExpression(astNode) {
                    if (astNode.callee && astNode.callee.object) {
                        if (astNode.callee.object.name === "node" && astNode.callee.property.name === "done") {
                            handleNodeDoneCall = false;
                        }
                    }
                }
            })
        }

        node.topic = n.topic;
        node.outstandingTimers = [];
        node.outstandingIntervals = [];
        node.clearStatus = false;

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
                name: node.name,
                path: node._path,
                outputCount: node.outputs,
                log: function() {
                    node.log.apply(node, arguments);
                },
                error: function() {
                    node.error.apply(node, arguments);
                },
                warn: function() {
                    node.warn.apply(node, arguments);
                },
                debug: function() {
                    node.debug.apply(node, arguments);
                },
                trace: function() {
                    node.trace.apply(node, arguments);
                },
                send: function(send, id, msgs, cloneMsg) {
                    sendResults(node, send, id, msgs, cloneMsg);
                },
                on: function() {
                    if (arguments[0] === "input") {
                        throw new Error(RED._("function.error.inputListener"));
                    }
                    node.on.apply(node, arguments);
                },
                status: function() {
                    node.clearStatus = true;
                    node.status.apply(node, arguments);
                }
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
            },
            flow: {
                set: function() {
                    node.context().flow.set.apply(node,arguments);
                },
                get: function() {
                    return node.context().flow.get.apply(node,arguments);
                },
                keys: function() {
                    return node.context().flow.keys.apply(node,arguments);
                }
            },
            global: {
                set: function() {
                    node.context().global.set.apply(node,arguments);
                },
                get: function() {
                    return node.context().global.get.apply(node,arguments);
                },
                keys: function() {
                    return node.context().global.keys.apply(node,arguments);
                }
            },
            env: {
                get: function(envVar) {
                    return RED.util.getSetting(node, envVar);
                }
            },
            setTimeout: function () {
                var func = arguments[0];
                var timerId;
                arguments[0] = function() {
                    sandbox.clearTimeout(timerId);
                    try {
                        func.apply(node,arguments);
                    } catch(err) {
                        node.error(err,{});
                    }
                };
                timerId = setTimeout.apply(node,arguments);
                node.outstandingTimers.push(timerId);
                return timerId;
            },
            clearTimeout: function(id) {
                clearTimeout(id);
                var index = node.outstandingTimers.indexOf(id);
                if (index > -1) {
                    node.outstandingTimers.splice(index,1);
                }
            },
            setInterval: function() {
                var func = arguments[0];
                var timerId;
                arguments[0] = function() {
                    try {
                        func.apply(node,arguments);
                    } catch(err) {
                        node.error(err,{});
                    }
                };
                timerId = setInterval.apply(node,arguments);
                node.outstandingIntervals.push(timerId);
                return timerId;
            },
            clearInterval: function(id) {
                clearInterval(id);
                var index = node.outstandingIntervals.indexOf(id);
                if (index > -1) {
                    node.outstandingIntervals.splice(index,1);
                }
            }
        };

        if (util.hasOwnProperty('promisify')) {
            sandbox.setTimeout[util.promisify.custom] = function(after, value) {
                return new Promise(function(resolve, reject) {
                    sandbox.setTimeout(function(){ resolve(value); }, after);
                });
            };
            sandbox.promisify = util.promisify;
        }
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
        const RESOLVING = 0;
        const RESOLVED = 1;
        const ERROR = 2;
        var state = RESOLVING;
        var messages = [];
        var processMessage = (() => {});

        node.on("input", function(msg,send,done) {
            if(state === RESOLVING) {
                messages.push({msg:msg, send:send, done:done});
            }
            else if(state === RESOLVED) {
                processMessage(msg, send, done);
            }
        });
        Promise.all(moduleLoadPromises).then(() => {
            var context = vm.createContext(sandbox);
            try {
                node.script = vm.createScript(functionText, createVMOpt(node, ""));
                var promise = Promise.resolve();

                processMessage = async function (msg, send, done) {
                    var start = process.hrtime();
                    context.msg = msg;
                    context.__send__ = send;
                    context.__done__ = done;

                    node.script.runInContext(context);

                    var inputMsg = context.msg;

                    try {
                        if (typeof inputMsg.beforeProc === 'string') {
                            await getExecResult(inputMsg.beforeProc, node.mydbConfig.con);
                        }

                        if (typeof inputMsg.procQuery === 'string') {
                            var rows = await getAllResult(inputMsg.procQuery, node.mydbConfig.con);
                            rows.forEach(async row => {
                                var resSql = inputMsg.proc(row);
                                await getExecResult(resSql, node.mydbConfig.con);
                            });
                        }

                        if (typeof inputMsg.afterProc === 'string') {
                            var response = await getAllResult(inputMsg.afterProc, node.mydbConfig.con);
                            msg.payload = response;
                        }
                        node.send(msg);
                    } catch(err) {
                        node.error(err, msg);
                        done(err);
                        return;
                    }

                    context.results.then(function(results) {
                        sendResults(node,send,msg._msgid,results,false);
                        if (handleNodeDoneCall) {
                            done();
                        }

                        var duration = process.hrtime(start);
                        var converted = Math.floor((duration[0] * 1e9 + duration[1])/10000)/100;
                        node.metric("duration", msg, converted);
                        if (process.env.NODE_RED_FUNCTION_TIME) {
                            node.status({fill:"yellow",shape:"dot",text:""+converted});
                        }
                    }).catch(err => {
                        if ((typeof err === "object") && err.hasOwnProperty("stack")) {
                            //remove unwanted part
                            var index = err.stack.search(/\n\s*at ContextifyScript.Script.runInContext/);
                            err.stack = err.stack.slice(0, index).split('\n').slice(0,-1).join('\n');
                            var stack = err.stack.split(/\r?\n/);

                            //store the error in msg to be used in flows
                            msg.error = err;

                            var line = 0;
                            var errorMessage;
                            if (stack.length > 0) {
                                while (line < stack.length && stack[line].indexOf("ReferenceError") !== 0) {
                                    line++;
                                }

                                if (line < stack.length) {
                                    errorMessage = stack[line];
                                    var m = /:(\d+):(\d+)$/.exec(stack[line+1]);
                                    if (m) {
                                        var lineno = Number(m[1])-1;
                                        var cha = m[2];
                                        errorMessage += " (line "+lineno+", col "+cha+")";
                                    }
                                }
                            }
                            if (!errorMessage) {
                                errorMessage = err.toString();
                            }
                            done(errorMessage);
                        }
                        else if (typeof err === "string") {
                            done(err);
                        }
                        else {
                            done(JSON.stringify(err));
                        }
                    });
                }

                node.on("close", function() {
                    while (node.outstandingTimers.length > 0) {
                        clearTimeout(node.outstandingTimers.pop());
                    }
                    while (node.outstandingIntervals.length > 0) {
                        clearInterval(node.outstandingIntervals.pop());
                    }
                    if (node.clearStatus) {
                        node.status({});
                    }
                });

                promise.then(function (v) {
                    var msgs = messages;
                    messages = [];
                    while (msgs.length > 0) {
                        msgs.forEach(function (s) {
                            processMessage(s.msg, s.send, s.done);
                        });
                        msgs = messages;
                        messages = [];
                    }
                    state = RESOLVED;
                }).catch((error) => {
                    messages = [];
                    state = ERROR;
                    node.error(error);
                });

            }
            catch(err) {
                // eg SyntaxError - which v8 doesn't include line number information
                // so we can't do better than this
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