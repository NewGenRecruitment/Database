var Database = require('../database');

var database = new Database({
    schema:      require('./schema.json')
  , credentials: "mongodb://newgen-rec-dev-b1c974d0:f23022eb537af3e5d87c1ec8e43656ba@ds053109.mongolab.com:53109/newgen-crm-dev"
});

database.connect(function (err) {

  if (err) {
    console.log('Database: Failed to connect!');
    return;
  }

  // Continue
  console.log('Database: Connected successfully!');

});