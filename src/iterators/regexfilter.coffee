BaseIterator = require('./baseiterator').BaseIterator

class RegExFilter extends BaseIterator
  constructor: (@options = {}) ->
    @options.name = @options.name ? 'RegExFilter'
    @options.priority = @options.priority ? 10
    @options.classname = @options.classname ? 'org.apache.accumulo.core.iterators.user.RegExFilter'

  getIteratorProperties: ->
    props = {}
    props.rowRegex ?= @options.rowRegex ? null
    props.colfRegex ?= @options.familyRegex ? null
    props.colqRegex ?= @options.qualifierRegex ? null
    props.valueRegex ?= @options.valueRegex ? null
    props.orFields = (@options.orFields ? false).toString().toLowerCase()
    props.matchSubstring = (@options.matchSubstring ? false).toString().toLowerCase()
    props

exports.RegExFilter = RegExFilter
