# nodeulo

A NPM package for Node.js interaction with Accumulo

## Requirements

* Running Accumulo cluster
* Accumulo Thrift proxy running
 * Due to limitations with the Node Thrift library, your proxy.properties has to have the `protocolFactory` option set to `org.apache.thrift.protocol.TBinaryProtocol$Factory`

## Usage

### Basic Table Operations
```javascript
var nodeulo = require('nodeulo')
var accumulo = new nodeulo.Accumulo({user: 'root', password: 'password'});

accumulo.connect(function() {
  accumulo.listTables(function(err, tables) {
    console.log(tables);
    accumulo.close();
  });
});
```

### Writing and Scanning
```javascript
var nodeulo = require('nodeulo')
var accumulo = new nodeulo.Accumulo({host: 'localhost', port: 12345});

accumulo.connect(function() {
  var mut = new nodeulo.Mutation('row');
  mut.put({columnFamily: 'fam', columnQualifier: 'qual', value: 'abcd'});
  accumulo.write('testtable', [mut], function() {
    accumulo.scan({table: 'testtable', columns: [{columnFamily: 'fam'}], function(err, results) {
      console.log(results);
      accumulo.close();
    });
  });
});
```

## Current Status

### Functional

* Initialization
* Table operations (list, create, rename, delete)
* Basic scanning
* Basic writing

### To Do

* More advanced writing
* More advanced scanning
* Iterators
* Examples
* Documentation
