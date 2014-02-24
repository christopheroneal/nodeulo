var nodeulo = require('nodeulo')

// initialize your accumulo object with information about your setup
var accumulo = new nodeulo.Accumulo({host: 'localhost', port: 12345, user: 'root', password: 'password'});

accumulo.connect(function() {
  var mut1 = new nodeulo.Mutation('row');
  mut.put({columnFamily: 'fam', columnQualifier: 'qual', value: 'abcd'});
  var mut2 = new nodeulo.Mutation('row2');
  mut2.put({columnFamily: 'fam', columnQualifier: 'something', value: 'qwer'});
  var muts = [mut1, mut2];
  accumulo.write('testtable', muts, function() {
    accumulo.scan({table: 'testtable', columns: [{columnFamily: 'fam'}], function(err, results) {
      // you should be able to see both of your new rows, since we scanned on the columnFamily,
      // which is the same.
      console.log(results);
      accumulo.close();
    });
  });
});
