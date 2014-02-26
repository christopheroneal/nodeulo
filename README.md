# nodeulo

[![NPM version](https://badge.fury.io/js/nodeulo.png)](http://badge.fury.io/js/nodeulo)
[![Dependency Status](https://gemnasium.com/christopheroneal/nodeulo.png)](https://gemnasium.com/christopheroneal/nodeulo)

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
    accumulo.createTable('testtable', function() {
      accumulo.listTables(function(err, newTables) {
        console.log(newTables);
        accumulo.close();
      });
    });
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

### Iterators and Ranges
```javascript
var nodeulo = require('nodeulo')
var accumulo = new nodeulo.Accumulo({});

accumulo.connect(function() {
  var muts = []
  for (i = 0 i < 20; i++) {
    var mut = new nodeulo.Mutation('test');
    mut.put({columnFamily: i, value: 'value'});
    muts.push(mut);
  }
  accumulo.write('testtable', muts, function() {
    var filter = new nodeulo.RegExFilter({familyRegex: '.*?0.*?'});
    var range = new nodeulo.Range({row: 'test', columnFamily: '5', include: true}, {row: 'test', columnFamily: '15', include: false});
    accumulo.scan({table: 'testtable', scanrange: range, iterators: [filter]}, function(err, results) {
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
* Simple examples
* Ranges
* Basic iterator functionality

### To Do

* More advanced writing
* More advanced scanning
* More iterators
* More examples
* Documentation
* Tests
