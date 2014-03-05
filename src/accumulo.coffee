thrift = require './thrift'
accumuloProxy = require './thrift-generated/AccumuloProxy'
accumuloTypes = require './thrift-generated/proxy_types'

Range = require('./range').Range
Mutation = require('./mutation').Mutation
BatchWriter = require('./batchwriter').BatchWriter

transport = thrift.TFramedTransport
protocol = thrift.TBinaryProtocol

class Accumulo
  constructor: (options) ->
    @host = options.host ? 'localhost'
    @port = options.port ? 42424
    @user = options.user ? 'root'
    @password = options.password ? 'secret'
    @connection = undefined
    @proxy = undefined
    @login = undefined

  connect: (callback) ->
    @connection = thrift.createConnection @host, @port, {transport: transport, protocol: protocol}
    @proxy = thrift.createClient accumuloProxy, @connection
    @proxy.login @user, {password: @password}, (err, conn) ->
      @login = conn
      callback err, @login

  close: ->
    @connection.end()

  listTables: (callback) ->
    @proxy.listTables @login, callback

  tableExists: (table, callback) ->
    @proxy.tableExists @login, table, callback

  createTable: (table, callback) ->
    @proxy.createTable @login, table
    callback

  deleteTable: (table, callback) ->
    @proxy.deleteTable @login, table, callback

  renameTable: (oldTable, newTable, callback) ->
    @proxy.renameTable @login, oldTable, newTable, callback

  getScanColumns: (cols) ->
    columns = null
    if cols 
      columns = []
      for col in cols
        sc = new accumuloTypes.ScanColumn()
        sc.colFamily = col.columnFamily
        sc.colQualifier = col.columnQualifier
        columns.push sc
    columns

  getRange: (r) ->
    r.out() ? null

  getIteratorSettings: (iters) ->
    iterators = null
    if iters
      iterators = []
      for i in iters
        iterators.push i.getIteratorSetting()
    iterators

  scan: (options, callback) ->
    scanOptions = new accumuloTypes.ScanOptions()
    scanOptions.range = getRange options.scanrange
    scanOptions.columns = getScanColumns options.columns
    scanOptions.iterators = getIteratorSettings options.iterators
    @proxy.createScanner @login, options.table, scanOptions, (err, scanner) ->
      performScan scanner, options.batchSize, (results) ->
        callback null, results

  performScan: (scanner, batchSize, callback) ->
    obj = []
    scanAll scanner, batchSize, obj, (results) ->
      callback results

  scanAll: (scanner, batchSize, obj, callback) ->
    @proxy.nextK scanner, batchSize, (err, ret) ->
      if err
        console.log(err)
      for item in ret.results
        obj.push item
      if ret.more
        scanAll scanner, batchSize, obj, callback
      else
        callback obj

  write: (table, mutations, callback) ->
    @createBatchWriter table, (bw) ->
      bw.addMutations mutations (res) ->
        bw.flush () ->
          bw.close callback

  createBatchWriter: (table, callback) ->
    bw = new BatchWriter options () ->
      callback bw

exports.Accumulo = Accumulo
