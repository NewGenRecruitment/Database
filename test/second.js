var database = require('../database').use('main-db');

module.exports.go = function () {

  console.log('Second->Go');

  database.model.employee.findOne({
      'login.username':    'josh.cole'
    , 'deleted.isDeleted': false
  })
  .exec(function (err, doc) {

    if (err) {
      console.log('Database: Query failed!', err);
      return;
    }

    console.log('Employee Doc:', doc);

  });

};