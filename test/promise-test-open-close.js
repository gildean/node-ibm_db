var common = require("./common")
  , odbc = require("../")
  , db = new odbc.Database()
  , assert = require("assert");

//assert.equal(db.connected, false);

db.query("select * from " + common.tableName).catch(function (err) {
  assert.deepEqual(err, { message: 'Connection not open.' });
  assert.equal(db.connected, false);
});

db.open(common.connectionString).then(function(res) {
  assert.equal(db.connected, true);
  return db.close();
}).then(function () {
  assert.equal(db.connected, false);
  return db.query("select * from " + common.tableName);
}).catch(function (err) {
  assert.deepEqual(err, { message: 'Connection not open.' });
  assert.equal(db.connected, false);
});
