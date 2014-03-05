accumuloProxy = require './thrift-generated/AccumuloProxy'
accumuloTypes = require './thrift-generated/proxy_types'

class BatchWriter
  constructor: (options, callback) ->
    @connection = options.connection
    writerOptions = new accumuloTypes.WriterOptions()
    writerOptions.maxMemory = options.maxMemory ? 10*1024
    writerOptions.latencyMs = options.latency ? 30*1000
    writerOptions.timeoutMs = options.timeout ? 5*1000
    writerOptions.threads = options.threads ? 10
    @isClosed = false
    @writer = undefined
    @connection.getProxy().createWriter @connection.getLogin(), options.table, writerOptions, (err, w) ->
      @writer = w
      callback w

  addMutations: (mutations, callback) ->
    if @isClosed
      throw "Can't write to a closed writer"
    cells = {}
    for mut in mutations
      cells[mut.row] = cells[mut.row] ? []
      for update in mut.updates
        cells[mut.row].push(update)
    @connection.getProxy().update(@writer, cells)
    callback

  addMutation: (mutation, callback) ->
    if @isClosed
      throw "Can't write to a closed writer"
    cells = {}
    cells[mutation.row] = mutation.updates
    @connection.getProxy().update(@writer, cells)
    callback

  flush: (callback) ->
    if @isClosed
      throw "Can't write to a closed writer"
    @connection.getProxy().flush(@writer)
    callback

  close: (callback) ->
    @connection.getProxy().closeWriter(@writer)
    @isClosed = true
    callback

exports.BatchWriter = BatchWriter
