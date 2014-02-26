var base = require('./baseiterator')
var util = require('../util')

function RegExFilter(options) {
  options = util.merge({
    name: 'RegExFilter',
    priority: 10,
    classname: 'org.apache.accumulo.core.iterators.user.RegExFilter',
    rowRegex: null,
    familyRegex: null,
    qualifierRegex: null,
    valueRegex: null,
    orFields: false,
    matchSubstring: false,
  }, options || {});
  this.options = options;
};

// this needs to be set immediately after the definition, but before getIteratorProperties
// otherwise, getIteratorProperties gets overridden by the base one
RegExFilter.prototype = new base.BaseIterator();

RegExFilter.prototype.getIteratorProperties = function() {
  props = {};
  if (this.options.rowRegex) {
    props.rowRegex = this.options.rowRegex;
  }
  if (this.options.familyRegex) {
    props.colfRegex = this.options.familyRegex;
  }
  if (this.options.qualifierRegex) {
    props.colqRegex = this.options.qualifierRegex;
  }
  if (this.options.valueRegex) {
    props.valueRegex = this.options.valueRegex;
  }
  props.orFields = this.options.orFields.toString().toLowerCase();
  props.matchSubstring = this.options.matchSubstring.toString().toLowerCase();
  return props;
};

exports.RegExFilter = RegExFilter;
