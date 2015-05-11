var thrift = require('thrift')
var accumuloProxy = require('./thrift-generated/AccumuloProxy')
var accumuloTypes = require('./thrift-generated/proxy_types')
var iterators = require('./iterators/iterators')

var transport = thrift.TFramedTransport;
var protocol = thrift.TBinaryTransport;

// helper method for defaulting values into options
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

// Mutation
// pass in row on init, use put to add different values for the remainder
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
  colup.colVisibility = options.columnVisibility;
  colup.timestamp = options.timestamp;
  colup.value = options.value;
  this.updates.push(colup);
};

function Range(start, end) {
  start = merge({
    row: null,
    columnFamily: null,
    columnQualifier: null,
    columnVisibility: null,
    timestamp: null,
    include: true,
  }, start || {});
  end = merge({
    row: null,
    columnFamily: null,
    columnQualifier: null,
    columnVisibility: null,
    timestamp: null,
    include: true,
  }, end || {});
  this.start = start;
  this.end = end;
}

Range.prototype.out = function() {
  range = new accumuloTypes.Range();
  range.startInclusive = this.start.include;
  range.stopInclusive = this.end.include;
  
  if (this.start.row) {
    range.start = new accumuloTypes.Key();
    range.start.row = this.start.row;
    range.start.colFamily = this.start.columnFamily;
    range.start.colQualifier = this.start.columnQualifier;
    range.start.timestamp = this.start.timestamp;
    range.start.colVisibility = this.start.columnVisibility;
  }
  if (this.end.row) {
    range.stop = new accumuloTypes.Key();
    range.stop.row = this.end.row;
    range.stop.colFamily = this.end.columnFamily;
    range.stop.colQualifier = this.end.columnQualifier;
    range.stop.timestamp = this.end.timestamp;
    range.stop.colVisibility = this.end.columnVisibility;
  }
  return range;
};

// BatchWriter
// used to write mutations into accumulo.
// options are defaulted, the only thing that is necessary to pass in is an 
// instance of Accumulo (see below) for connection, etc.
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
  // this callback was necessary because i was getting through an entire write without the writer
  // being initialized.  this will keep us from getting anywhere without having that
  this.connection.getProxy().createWriter(this.connection.getLogin(), options.table, writerOptions, function(err, w) {
    writer = w;
    callback(w);
  });
  this.is_closed = false;

  // add mutations to the writer
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
    this.connection.getProxy().update(writer, cells, callback);
    callback();
  } 

  // add a single mutation to the writer
  this.addMutation = function(mutation, callback) {
    if (this.is_closed) {
      throw "Can't write to a closed writer"
    }
    cells ={};
    cells[mutation.row] = mutation.updates;
    this.connection.getProxy().update(writer, cells);
    callback();
  }
  
  // flush the updates
  this.flush = function(callback) {
    if (this.is_closed) {
      throw "Can't flush a closed writer"
    }
    this.connection.getProxy().flush(writer);
    callback();
  }

  // close the writer
  this.close = function(callback) {
    this.connection.getProxy().closeWriter(writer);
    this.is_closed = true;
    callback();
  }
};

// Accumulo
// this is where most of the magic happens
// modify options to fit your instance, must have thrift proxy running
// to use.
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

  // connect to the accumulo instance
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
  
  // close connection to accumulo instance
  // if you don't do this, your program will hang
  this.close = function() {
    connection.end();
  }
  
  // list all tables in accumulo
  // callback should be of the format function(err, tables)
  this.listTables = function(callback) {
    proxy.listTables(login, callback);
  }
  
  // return whether a given table exists
  // callback should be of the format function(err, exists)
  this.tableExists = function(table, callback) {
    proxy.tableExists(login, table, callback);
  }
  
  // create a table
  // callback is for consistency, nothing is returned
  this.createTable = function(table, callback) {
    // for some reason, unlike the rest of the table ops, createTable
    // doesn't get generated with a callback, so we'll add one for consistency
    proxy.createTable(login, table);
    callback();
  }
  
  // delete a table
  // callback should be of the format function(err)
  this.deleteTable = function(table, callback) {
    proxy.deleteTable(login, table, callback);
  }

  // rename a table
  // callback should be of the format function(err)
  this.renameTable = function(oldtable, newtable, callback) {
    proxy.renameTable(login, oldtable, newtable, callback);
  }
  
  // internal method used to turn column option into ScanColumn objects
  var getScanColumns = function(cols) {
    columns = null;
    if (cols != null) {
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
  
  // internal method to get the accumulo representation of a range to pass
  // to a scanner
  var getRange = function(r) {
    return r ? r.out() : null;
  };

  // internal method to get the accumulo representation of iterator settings
  var getIteratorSettings = function(iters) {
    var iterators = null;
    if (iters != null) {
      iterators = [];
      for (i = 0; i < iters.length; i++) {
        iterators.push(iters[i].getIteratorSetting());
      }
    }
    return iterators;
  };

  // scan an accumulo table
  // options requires the table value, other stuff can be defaulted
  // - can limit scan to column family/qualifier by passing in array of
  // objects with columnFamily and/or columnQualifier values
  // - can limit by range
  // - can include iterators
  //
  // this will return all values matching (be aware before kicking off big scan)
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
    scanOptions.range = getRange(options.scanrange);
    scanOptions.columns = getScanColumns(options.columns);
    scanOptions.iterators = getIteratorSettings(options.iterators);
    proxy.createScanner(login, options.table, scanOptions, function(err, scanner) {
      performScan(scanner, options.batchSize, function(results) {
        callback(null, results);
      });
    });
  }

  // internal method to assist in the scanning
  var performScan = function(scanner, batchSize, callback) {
    var obj = [];
    scanAll(scanner, batchSize, obj, function(results) {
      callback(results)
    });
  }
  
  // internal method to get all values on a scan
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
 
  // write mutations to a table
  // this will do the work of creating a BatchWriter for you and all
  this.write = function(table, mutations, callback) {
    this.createBatchWriter({table: table, connection: this}, function(bw) {
      bw.addMutations(mutations, function(res) {
        bw.flush(function() {
          bw.close(callback);
        });
      });
    });
  }
  
  // create a BatchWriter
  // callback should be of the format function(BatchWriter)
  this.createBatchWriter = function(options, callback) {
    var bw = new BatchWriter(options, function() {
      callback(bw);
    });
  }
};

exports.Accumulo = Accumulo;
exports.BatchWriter = BatchWriter;
exports.Mutation = Mutation;
exports.Range = Range;

exports.BaseIterator = iterators.BaseIterator;
exports.RegExFilter = iterators.RegExFilter;
