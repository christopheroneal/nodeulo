var accumuloTypes = require('./thrift-generated/proxy_types');
var merge = require('./util').merge;

// Range
// 
//
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
};

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

exports.Range = Range;
