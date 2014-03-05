accumuloTypes = require './thrift-generated/proxy_types'

class Range
  constructor: (@start, @end) ->

  out: ->
    r = new accumuloTypes.Range()
    r.startInclusive = @start.include ? true
    r.endInclusive = @end.include ? true

    if @start.row
      r.start = new accumuloTypes.Key()
      r.start.row = @start.row
      r.start.colFamily = @start.columnFamily ? null
      r.start.colQualifier = @start.columnQualifier ? null
      r.start.colVisibility = @start.columnVisibility ? null
      r.start.timestamp = @start.timestamp ? null
      
    if @end.row
      r.stop = new accumuloTypes.Key()
      r.stop.row = @end.row
      r.stop.colFamily = @end.columnFamily ? null
      r.stop.colQualifier = @end.columnQualifier ? null
      r.stop.colVisibility = @end.columnVisibility ? null
      r.stop.timestamp = @end.timestamp ? null

exports.Range = Range
