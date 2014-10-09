/*
 * Connects to the database, prepares models and provides various useful methods.
 */

var ME            = module.exports
  , mongoose      = require('mongoose')
  , _             = require('underscore')
  , schemaBuilder = require('./schemaBuilder')
  , config        = require('./config');

ME.conx              = null;
ME.isConnectedFlag   = false;
ME.onConnectHandlers = [];
ME.enableDebug       = false;   //manually enable Mongoose debug output (only applies in development)
ME.model             = {};

/*
 * Connects and sets up the database ready for use.
 */
ME.connect = function (primaryCallback) {

  if (ME.onConnectHandlers === null) ME.onConnectHandlers = [];

  // Add the primary callback, if any
  if (typeof primaryCallback === 'function')
    ME.onConnectHandlers.unshift(primaryCallback);

  // When debugging, this will output all calls mongoose makes.
  // To enable debugging set enableDebug to true.
  if (ME.enableDebug && config.environment.level === 3)
    mongoose.set('debug', true);

  // Connect!
  mongoose.connect(config.databaseCredentials);
  ME.conx = mongoose.connection;

  // Event listener to listen for an error on connection
  ME.conx.on('error', function (err) {

    console.log('Database Error!');
    console.log(err);

    // Pass error to on connect handlers
    if (ME.onConnectHandlers && ME.onConnectHandlers.length > 0) {
      for (var h = 0 ; h < ME.onConnectHandlers.length ; h++) {
        ME.onConnectHandlers[h](err);
      }
      ME.onConnectHandlers = null;
    }

  });

  // Once the connection is opened, set mongo models
  ME.conx.once('open', function () {

    // Convert the schema to mongoose format
    schemaBuilder.build(__dirname + '/../schema.json', function (err, mongooseModels) {

      if (!err) {
        // Store the mongoose models
        ME.model = mongooseModels;

        // Mark as connected
        ME.isConnectedFlag = true;
      }

      // Run on connect handlers (passing schema error, if any)
      if (ME.onConnectHandlers.length > 0) {
        for (var h = 0 ; h < ME.onConnectHandlers.length ; h++) {
          ME.onConnectHandlers[h](err, ME);
        }
        ME.onConnectHandlers = null;
      }

    });

  });

};

/*
 * Disconnects from the database.
 */
ME.disconnect = function (callback) {
  return mongoose.disconnect(callback);
};

/*
 * Stores a handler to be run when the connection is ready.
 * onConnectHandler(schemaErr, database)
 */
ME.onConnected = function (fn) {
  if (typeof fn !== 'function') return;

  // Store for later
  if (ME.isConnectedFlag === false) {
    ME.onConnectHandlers.push(fn);
  }

  // Already connected, run it now
  else {
    fn(null, ME);
  }

};

/*
 * Returns true if the database is connected.
 */
ME.isConnected = function () {
  return ME.isConnectedFlag;
};

/*
 * Pushes an object ID into a given document's property array. The callback
 * parameter is optional.
 * callback(err, doc);
 */
ME.pushReference = function (doc, fieldName, value, callback) {

  // Push onto arrays, replace all other types
  if (_.isArray(doc[fieldName])) { doc[fieldName].push(value); }
  else { doc[fieldName] = value; }

  // Save it
  doc.save(function (err) {
    return callback(err, doc);
  });

};

/*
 * Gets a single document by its ID alone.
 */
ME.getById = function (collectionName, id, callback) {

  // Setup query to return one item
  var query = ME.model[collectionName].findOne({
      _id:                 id
    , 'deleted.isDeleted': false
  });

  // Run the query and pass the document to the callback
  query.exec(callback);

};

/*
 * Gets the maximum value of the given collection/field. If 'getting' param is
 * set to 'min' this will return the minimum value instead.
 * callback(err, maxValue, doc)
 */
ME.getMax = function (collectionName, fieldName, conditions, callback, getting) {
  conditions = conditions || {};
  getting    = getting    || 'max';

  // Setup query to return one item
  var query = ME.model[collectionName].findOne(conditions);

  // Sort DESC (so we only find the top one)
  var operand = (getting === 'max' ? '-' : (getting === 'min' ? '+' : ''));
  query.sort(operand + fieldName);

  // No callback, return thr query instead
  if (typeof callback !== 'function') return query;

  // Run the query and pass the max value to the callback
  query.exec(function (err, doc) {
    var maxValue = (!err && doc ? doc[fieldName] : null);
    return callback(err, maxValue, doc);
  });

};

/*
 * Gets the minimum value of the given collection/field.
 * callback(err, minValue, doc)
 */
ME.getMin = function (collectionName, fieldName, conditions, callback) {
  return ME.getMax(collectionName, fieldName, conditions, callback, 'min');
};

/*
 * Counts the values in the given field(s). For number fields the count is
 * incremented by the number value, for string fields the count is incremented
 * by 1. The callback is passed a hash 'count' containing counters for each
 * given field.
 * Alternatively if a falsy value is provided to 'fields' the method will
 * count the number of matching documents and will return a single integer.
 * [count:fields]
 *  fieldName1: 18
 *  fieldName2: 0
 *  fieldName3: 2
 * [count:documents]
 *  (returns integer)
 * callback(err, count)
 */
ME.count = function (collectionName, fields, conditions, callback) {
  conditions = conditions || {};
  if (typeof fields === 'string') fields = [fields];  //ensure fields is an array of strings

  var mode  = (fields ? 'fields' : 'documents')
    , count = {};

  // Prepare for counting fields
  if (mode === 'fields') {
    for (var f in fields) {
      var fieldName = fields[f];
      count[fieldName] = 0;
    }
  }

  // Setup query to return all matching documents
  var query = ME.model[collectionName].find(conditions);

  // Run the query and pass the max value to the callback
  query.exec(function (err, docs) {

    if (err) return callback(err);

    // Counting documents, easy peasy
    if (mode === 'documents') return callback(null, docs.length);

    // Cycle each document
    for (var d = 0, dlen = docs.length ; d < dlen ; d++) {
      var doc = docs[d];

      // Count each given field
      for (var fieldName in count) {
        switch (typeof doc[fieldName]) {
          case 'number': count[fieldName] += doc[fieldName]; break;
          case 'string': count[fieldName] += 1;              break;
        }
      }

    }

    // Pass back
    return callback(null, count);

  });

};

/*
 * Returns a new object ID to be used when adding new documents (optional).
 */
ME.newObjectId = function () {
  return mongoose.Types.ObjectId();
};

/*
 * Returns true if the variable is an ObjectID.
 */
ME.isObjectId = function (input) {
  var regexp = new RegExp("^[0-9a-fA-F]{24}$");
  return regexp.test(input);
};

/*
 * Converts a string to an ObjectID.
 */
ME.toObjectId = function (input) { return mongoose.Types.ObjectId(input); };

/*
 * Converts an array of object IDs to an array of strings.
 */
ME.objectIdArrayToString = function (input) {
  var newArr = [];
  for (var i = 0, ilen = input.length ; i < ilen ; i++) {
    newArr.push(input[i].toString());
  }
  return newArr;
};

/*
 * Returns true if the specified array contains the specified ObjectID.
 */
ME.containsObjectId = function (arr, objectId, property) {
  if (typeof property === 'undefined') property = '_id';

  var index = null;

  for (var i=0 ; i < arr.length ; i++) {
    var arrItem        = arr[i]
      , isItemObjectId = ME.isObjectId(arrItem)
      , validObjectId  = false
      , value;

    // Passed in an object
    if (!isItemObjectId && typeof arrItem === 'object') {
      validObjectId = ME.isObjectId(arrItem[property]);
      value         = arrItem[property];
    }

    // Passed in an array of object IDs
    else {
      validObjectId = isItemObjectId;
      value         = arrItem;
    }
    // If the array element is an object Id the values are compared using the
    // MongoDB equals method, otherwise they are compared naturally.
    if (
      (validObjectId && value.equals(objectId)) ||
      (!validObjectId && value == objectId)   //use == not === as we can't be sure of the data types here [ e.g. "1" == 1 ]
    ) {
      index = i;
      break;
    }
  }

  return (index !== null);
};