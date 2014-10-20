var database       = require('../database')
  , DatabaseSchema = require('./schema.json')
  , second         = require('./second');

database.connect({
    schema:      DatabaseSchema
  , credentials: "mongodb://newgen-rec-dev-b1c974d0:f23022eb537af3e5d87c1ec8e43656ba@ds053109.mongolab.com:53109/newgen-crm-dev"
}, function (err) {

  if (err) {
    console.log('Database: Failed to connect!');
    return;
  }

  console.log('Database: Connected successfully!');

  second.go();

});