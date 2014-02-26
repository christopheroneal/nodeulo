var accumuloTypes = require('./thrift-generated/proxy_types');
var merge = require('./util').merge;

// Mutation
// pass in row on init, use put to add different values for the remainder
//
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
  var colup = new accumuloTypes.ColumnUpdate();
  colup.colFamily = options.columnFamily;
  colup.colQualifier = options.columnQualifier;
  colup.timestamp = options.timestamp;
  colup.value = options.value;
  this.updates.push(colup);
};

exports.Mutation = Mutation;
