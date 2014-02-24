var thrift = require('thrift')
var accumuloProxy = require('./thrift-generated/AccumuloProxy')
var accumuloTypes = require('./thrift-generated/proxy_types')

var transport = thrift.TFramedTransport;
var protocol = thrift.TBinaryTransport;
var connection = exports.connection = undefined;
var proxy = exports.proxy = undefined;
var login = exports.login = undefined;

exports.connect = function(host, port, user, password, callback) {
  connection = thrift.createConnection(host, port, {
    transport: transport, 
    protocol: protocol
  });
  proxy = thrift.createClient(accumuloProxy, connection):
  proxy.login(user, {password: password}, function(err, conn) {
    if (err) {
      console.log(err);
      return;
    } else {
      login = conn;
      callback(login);
    }
  }); 
}

exports.close = function() {
  connection.end();
}

exports.list_tables = function(callback) {
  proxy.listTables(login, callback);
}

exports.table_exists = function(table, callback) {
  proxy.tableExists(login, table, callback);
}

exports.create_table = function(table, callback) {
  proxy.createTable(login, table, callback);
}

exports.delete_table = function(table, callback) {
  proxy.deleteTable(login, table, callback);
}

exports.rename_table = function(oldtable, newtable, callback) {
  proxy.renameTable(login, oldtable, newtable, callback);
}

