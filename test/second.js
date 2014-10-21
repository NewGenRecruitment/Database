var database = require('../database').use('main-db');

module.exports.go = function () {

  console.log('Second->Go');

  database.model.staff.findOne({
      loginEmail:          'josh.cole@newgenrecruitment.com'
    , 'deleted.isDeleted': false
  })
  .exec(function (err, doc) {

    if (err) {
      console.log('Database: Query failed!', err);
      return;
    }

    console.log('Staff Doc:', doc);

  });

};