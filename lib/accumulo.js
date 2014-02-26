var thrift = require('thrift')
var accumuloProxy = require('./thrift-generated/AccumuloProxy')
var accumuloTypes = require('./thrift-generated/proxy_types')

var merge = require('./util').merge

var Range  = require('./range').Range
var Mutation = require('./mutation').Mutation
var BatchWriter = require('./batchwriter').BatchWriter

var transport = thrift.TFramedTransport;
var protocol = thrift.TBinaryTransport;

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
