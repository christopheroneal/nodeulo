accumuloTypes = require '../thrift-generated/proxy_types'

class BaseIterator
  constructor: (@options) ->

  getIteratorProperties: ->
    {}

  getIteratorSetting: ->
    i = new accumuloTypes.IteratorSetting()
    i.name = @options.name
    i.priority = @options.priority
    i.iteratorClass = @options.classname
    i.properties = @getIteratorProperties()
    i
  
exports.BaseIterator = BaseIterator
