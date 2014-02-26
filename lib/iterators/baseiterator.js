var accumuloTypes = require ('../thrift-generated/proxy_types')

// this is the base iterator that all other iterators will inherit from. 
// provides basic functions for dealing with the iterators in nodeulo
function BaseIterator(options) {
  this.options = options;
};

BaseIterator.prototype.getIteratorSetting = function() {
  var i = new accumuloTypes.IteratorSetting();
  i.name = this.options.name;
  i.priority = this.options.priority;
  i.iteratorClass = this.options.classname;
  i.properties = this.getIteratorProperties();
  return i;
};

BaseIterator.prototype.getIteratorProperties = function() {
  return {};
};

exports.BaseIterator = BaseIterator;
