var database = require('../database').use('main-db');

module.exports.go = function () {

  database.model.staff.findOne({
      loginEmail: 'josh.cole@newgenrecruitment.com'
  })
  .exec(function (err, doc) {

    if (err) {
      console.log('Database: Query failed!', err);
      return;
    }

    console.log('Staff Doc:', doc);

  });

};