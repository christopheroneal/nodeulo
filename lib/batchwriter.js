var accumuloProxy = require('./thrift-generated/AccumuloProxy')
var accumuloTypes = require('./thrift-generated/proxy_types')
var merge = require('./util').merge

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
    cells = {};
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
      throw "Can't write to a closed writer";
    }
    cells ={};
    cells[mutation.row] = mutation.updates;
    this.connection.getProxy().update(writer, cells);
    callback();
  }
  
  // flush the updates
  this.flush = function(callback) {
    if (this.is_closed) {
      throw "Can't flush a closed writer";
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

exports.BatchWriter = BatchWriter;
