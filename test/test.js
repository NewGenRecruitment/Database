var databaseModule     = require('../database')
  , DatabaseConnection = databaseModule.Connection
  , DatabaseSchema     = require('./schema.json');

var database = new DatabaseConnection({
    schema:      DatabaseSchema
  , credentials: "mongodb://newgen-rec-dev-b1c974d0:f23022eb537af3e5d87c1ec8e43656ba@ds053109.mongolab.com:53109/newgen-crm-dev"
});

database.connect(function (err) {

  if (err) {
    console.log('Database: Failed to connect!');
    return;
  }

  console.log('Database: Connected successfully!', database);

  database.model.staff.findOne({ loginEmail: 'josh.cole@newgenrecruitment.com' }, function (err, doc) {

    if (err) {
      console.log('Database: Query failed!', err);
      return;
    }

    console.log('Staff Doc:', doc);

  });

});