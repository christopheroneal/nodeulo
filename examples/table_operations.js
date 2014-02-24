var nodeulo = require('nodeulo')

// initialize your accumulo object with information about your setup
var accumulo = new nodeulo.Accumulo({host: 'localhost', port: 12345, user: 'root', password: 'password'});

accumulo.connect(function() {
  accumulo.listTables(function(err, tables) {
    // should display initial set of tables in your accumulo instance
    console.log(tables);
    accumulo.createTable('test', function(err) {
      accumulo.listTables(function(err, tables) {
        // now you've got a new table named 'test'
        console.log(tables);
        accumulo.deleteTable('test', function(err) {
          accumulo.listTables(function(err, tables) {
            // goodbye test
            console.log(tables);
            accumulo.close();
          });
        })'
      });
    });
  });
});
