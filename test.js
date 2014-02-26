var nodeulo = require('./lib/nodeulo')

var mut = new nodeulo.Mutation('poop');
mut.put({columnFamily: 'aaa', value: 'val'});
console.log(mut);
var r = new nodeulo.Range({row: 'qqq'}, {row: 'wwww'});
console.log(r);

var reg = new nodeulo.RegExFilter();
console.log(reg);

var accumulo = new nodeulo.Accumulo();
console.log(accumulo);

var bw = new nodeulo.BatchWriter({connection: accumulo});
console.log(bw);
