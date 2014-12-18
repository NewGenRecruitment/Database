var databaseModule     = require('../database')
  , DatabaseConnection = databaseModule.Connection
  , databaseSchema     = require('ng-database-schema')
  , second             = require('./second');

console.log('Schema: Preparing...');

databaseSchema.prepare(function (err, schema) {

  if (err) {
    console.log('Schema: Failed to prepare!', err);
    return;
  }

  console.log('Schema: Ready.');
  console.log('Database: Connecting...');

  var database = new DatabaseConnection({
      dbId:        'main-db'
    , schema:      schema
    , credentials: "mongodb://newgen-crm2-dev-3e37f3225a6ad957be9feed2:b30eda667e127fd2a6b4df9764843223e3fd0ab9e1aeeae91000df47af24bf0e@ds027491.mongolab.com:27491/newgen-crm2-dev"
  });

  database.connect(function (err) {

    if (err) {
      console.log('Database: Failed to connect!', err);
      return;
    }

    console.log('Database: Ready.');

    second.go();

  });

});