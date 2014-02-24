var thrift = require('thrift')
var accumuloProxy = require('./thrift-generated/AccumuloProxy')
var accumuloTypes = require('./thrift-generated/proxy_types')

var transport = thrift.TFramedTransport;
var protocol = thrift.TBinaryTransport;

function merge() {
  var obj, name, copy,
    target = arguments[0] || {},
    i = 1,
    length = arguments.length;

  for (; i < length; i++) {
    if ((obj = arguments[i]) != null) {
      for (name in obj) {
        copy = obj[name];

        if (target === copy) {
          continue;
        }
        else if (copy !== undefined) {
          target[name] = copy;
        }
      }
    }
  }

  return target;
};

function Mutation(row) {
  this.row = row;
  this.updates = [];
};

Mutation.prototype.put = function(options) {
  options = merge({
    columnFamily: '',
    columnQualifier: '',
    columnVisibility: null,
    timestamp: null,
    value: '',
  }, options || {});
  colup = new accumuloTypes.ColumnUpdate();
  colup.colFamily = options.columnFamily;
  colup.colQualifier = options.columnQualifier;
  colup.timestamp = options.timestamp;
  colup.value = options.value;
  this.updates.push(colup);
};

function BatchWriter(options, callback) {
  options = merge({
    maxMemory: 10*1024,
    latency: 30*1000,
    timeout: 5*1000,
    threads: 10,
  }, options || {});
  this.connection = options.connection;
  writerOptions = new accumuloTypes.WriterOptions();
  writerOptions.maxMemory = options.maxMemory;
  writerOptions.latencyMs = options.latency;
  writerOptions.timeoutMs = options.timeout;
  writerOptions.threads = options.threads;
  var writer = undefined;
  this.connection.getProxy().createWriter(this.connection.getLogin(), options.table, writerOptions, function(err, w) {
    writer = w;
    callback(w);
  });
  this.is_closed = false;

  this.addMutations = function(mutations, callback) {
    if (this.is_closed) {
      throw "Can't write to a closed writer"
    }
    cells = {}
    for (i = 0; i < mutations.length; i++) {
      cells[mutations[i].row] = typeof cells[mutations[i].row] !== 'undefined' ? cells[mutations[i].row] : [];
      for (j = 0; j < mutations[i].updates.length; j++) {
        cells[mutations[i].row].push(mutations[i].updates[j]);
      }
    }
    console.log('adding mutations'); 
    console.log(cells);
    console.log(writer);
    this.connection.getProxy().update(writer, cells, callback);
    callback();
  } 

  this.addMutation = function(mutation, callback) {
    if (this.is_closed) {
      throw "Can't write to a closed writer"
    }
    cells ={};
    cells[mutation.row] = mutation.updates;
    this.connection.getProxy().update(writer, cells);
    callback();
  }
  
  this.flush = function(callback) {
    if (this.is_closed) {
      throw "Can't flush a closed writer"
    }
    this.connection.getProxy().flush(writer);
    callback();
  }

  this.close = function(callback) {
    console.log('closing');
    console.log(writer);
    this.connection.getProxy().closeWriter(writer);
    this.is_closed = true;
    callback();
  }
};

function Accumulo(options) {
  options = merge({
    host: 'localhost',
    port: 42424,
    user: 'root',
    password: 'secret',
  }, options || {});
  this.host = options.host;
  this.port = options.port;
  this.user = options.user;
  this.password = options.password;
  var connection = undefined;
  var proxy = undefined;
  var login = undefined;

  this.getLogin = function() {
    return login;
  }
  
  this.getProxy = function() {
    return proxy;
  }

  this.connect = function(callback) {
    connection = thrift.createConnection(this.host, this.port, {
      transport: transport,
      protocol: protocol,
    });
    proxy = thrift.createClient(accumuloProxy, connection);
    proxy.login(this.user, {password: this.password}, function(err, conn) {
      login = conn;
      callback(err, login);
    });
  }
  
  this.close = function() {
    connection.end();
  }
  
  this.listTables = function(callback) {
    proxy.listTables(login, callback);
  }
  
  this.tableExists = function(table, callback) {
    proxy.tableExists(login, table, callback);
  }
  
  this.createTable = function(table, callback) {
    // for some reason, unlike the rest of the table ops, createTable
    // doesn't get generated with a callback, so we'll add one for consistency
    proxy.createTable(login, table);
    callback();
  }
  
  this.deleteTable = function(table, callback) {
    proxy.deleteTable(login, table, callback);
  }

  this.renameTable = function(oldtable, newtable, callback) {
    proxy.renameTable(login, oldtable, newtable, callback);
  }
  
  var getScanColumns = function(cols) {
    columns = null;
    if (columns != null) {
      columns = [];
      for (i = 0; i < cols.length; i++) {
        var sc = new accumuloTypes.ScanColumn();
        sc.colFamily = cols[i].columnFamily;
        sc.colQualifier = cols[i].columnQualifier;
        columns.push(sc);
      }
    }
    return columns;
  }

  this.scan = function(options, callback) {
    options = merge({
      scanrange: null,
      columns: null, 
      authorizations: null,
      iterators: null,
      bufferSize: null,
      batchSize: 10,
    }, options || {});
    scanOptions = new accumuloTypes.ScanOptions();
    scanOptions.columns = getScanColumns(options.columns);
    proxy.createScanner(login, options.table, scanOptions, function(err, scanner) {
      performScan(scanner, options.batchSize, function(results) {
        callback(null, results);
      });
    });
  }

  var performScan = function(scanner, batchSize, callback) {
    var obj = [];
    scanAll(scanner, batchSize, obj, function(results) {
      callback(results)
    });
  }
  
  var scanAll = function(scanner, batchSize, obj, callback) {
    proxy.nextK(scanner, batchSize, function(err, ret) {
      if (err) {
        console.log(err);
      }
      for (i = 0; i < ret.results.length; i++) {
        var item = ret.results[i];
        obj.push(item);
      }
      if (ret.more) {
        scanAll(scanner, batchSize, obj, callback);
      } else {
        callback(obj);
      }
    });
  }
 
  this.write = function(table, mutations, callback) {
    this.createBatchWriter({table: table, connection: this}, function(bw) {
      console.log(bw);
      bw.addMutations(mutations, function(res) {
        console.log('in callback');
        bw.flush(function() {
          bw.close(callback);
        });
      });
    });
  }
  
  this.createBatchWriter = function(options, callback) {
    var bw = new BatchWriter(options, function() {
      callback(bw);
    });
  }
};

exports.Accumulo = Accumulo;
exports.BatchWriter = BatchWriter;
exports.Mutation = Mutation;
