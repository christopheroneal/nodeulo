accumuloTypes = require './thrift-generated/proxy_types'

class Mutation
  constructor: (@row) ->
    @updates = []

  put: (options = {}) ->
    colup = new accumuloTypes.ColumnUpdate()
    colup.colFamily = options.columnFamily ? ''
    colup.colQualifier = options.columnQualifier ? ''
    colup.colVisibility = options.columnVisibility ? ''
    colup.timestamp = options.timestamp ? null
    colup.value = options.value ? ''
    @updates.push colup

exports.Mutation = Mutation
