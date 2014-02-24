# nodeulo

A NPM package for Node.js interaction with Accumulo

## Requirements

* Running Accumulo cluster
* Accumulo Thrift proxy running
 * Due to limitations with the Node Thrift library, your proxy.properties has to have the `protocolFactory` option set to `org.apache.thrift.protocol.TBinaryProtocol$Factory`

## Usage

```javascript
var nodeulo = require('nodeulo')

nodeulo.connect('localhost', 42424, 'root', 'password', function() {
  nodeulo.list_tables(function(err, tables) {
    console.log(tables);
    nodeulo.close();
  });
});
```

## Current Status

### Functional

* Initialization
* Table operations (list, create, rename, delete)

### To Do

* Writing
* Scanning
* Iterators
* Examples
* Documentation
