var ME            = module.exports
  , extender      = require('ng-extender')
  , logger        = require('ng-logger')
  , schemaBuilder = require('ng-schema-builder')
  , mongoose      = require('mongoose')
  , _             = require('underscore');

ME.credentials       = null;
ME.debug             = false;
ME.conx              = null;
ME.isConnectedFlag   = false;
ME.onConnectHandlers = [];
ME.model             = {};

/*
 * Connect to the database.
 * callback(err, database)
 * handler(err, database)
 * [options]
 *  schema      (str/obj)    Either the absolute path to a schema file OR a schema object.
 *  credentials (string)     A MongoDB connection string.
 *  debug       (bool>false) Set true to manually enable Mongoose debug output.
 */
ME.connect = function (options, callback) {
  if (!_.isFunction(callback)) callback = function(){};

  // Default option values
  options = extender.extend({
      schema:      null
    , credentials: null
    , debug:       false
  }, options);

  // Already connected!
  if (ME.isConnectedFlag) return callback(null, ME);

  // Store for later
  ME.credentials = options.credentials;
  ME.debug       = options.debug;

  // Build the schema for the first time
  if (_.keys(ME.model).length === 0) {
    ME.rebuildSchema(options.schema, function (err) {
      if (err) return callback(err);
      return ME.connect(options, callback);  //drop out of this method & re-enter from the top
    });
    return;
  }

  // Add the primary callback, if any
  if (_.isFunction(callback)) ME.onConnectHandlers.unshift(callback);

  // When debugging, this will output all calls mongoose makes.
  if (ME.debug) mongoose.set('debug', true);

  // Connect!
  mongoose.connect(ME.credentials);
  ME.conx = mongoose.connection;

  // Error handler, pass the error to all connection handlers
  ME.conx.on('error', function (err) {

    logger.error('Database Error!').error(err);

    ME.passToConnectionHandlers(ME.onConnectHandlers, err);
    ME.isConnectedFlag = false;

  });

  // Success handler, pass the database object to all connection handlers
  ME.conx.once('open', function () {

    ME.isConnectedFlag = true;

    ME.passToConnectionHandlers(ME.onConnectHandlers);
    ME.onConnectHandlers = [];

  });

  ME.conx.on('connected', function () {
    ME.isConnectedFlag = true;
  });

  ME.conx.on('disconnected', function () {
    ME.isConnectedFlag = false;
  });

};

/*
 * Takes an array of connection handlers and fires them, passing in the error,
 * if any.
 */
ME.passToConnectionHandlers = function (handlers, err) {
  err = err || null;

  if (handlers && handlers.length > 0) {
    for (var h = 0 ; h < handlers.length ; h++) {
      handlers[h](err, ME);
    }
  }

};

/*
 * [Re]builds the Mongoose schema from the short-hand JSON format.
 * callback(err);
 */
ME.rebuildSchema = function (schema, callback) {
  if (!_.isFunction(callback)) callback = function(){};

  // Convert the short-hand schema to Mongoose format
  schemaBuilder.build(schema, function (err, mongooseModels) {

    if (err) return callback(err);

    // Successfully merge in the models
    ME.model = mongooseModels;
    return callback(null);

  });

};

/*
 * Toggle Mongoose debug output.
 */
ME.setDebug = function (debug) {
  ME.debug = debug;
  mongoose.set('debug', debug);
};

/*
 * Disconnects from the database.
 * callback();
 */
ME.disconnect = function (callback) {
  if (!_.isFunction(callback)) callback = function(){};

  return mongoose.disconnect(function () {
    ME.isConnectedFlag = false;
    return callback();
  });

};

/*
 * Stores a handler to be run when the connection is ready.
 * onConnectHandler(err, database)
 */
ME.onConnected = function (fn) {
  if (!_.isFunction(fn)) return;

  // Run it now if we are connected and all other connection handlers have been dealt with
  if (ME.isConnectedFlag && ME.onConnectHandlers.length === 0)
    return fn(null, ME);

  // Otherwise store it for later
  ME.onConnectHandlers.push(fn);

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
 * callback(err, doc)
 */
ME.pushReference = function (doc, fieldName, value, callback) {

  // Push onto arrays, for all other types replace the value
  if (_.isArray(doc[fieldName])) { doc[fieldName].push(value); }
  else                           { doc[fieldName] = value; }

  doc.save(function (err) {
    return callback(err, doc);
  });

};

/*
 * Gets a single document by its ID alone, ensuring only non-deleted records are
 * returned.
 * callback(err, doc)
 */
ME.findById = function (collectionName, id, callback) {

  // Setup query to return one item
  ME.model[collectionName].findOne({
      _id:                 id
    , 'deleted.isDeleted': false
  })
  .exec(callback);

};

/*
 * Gets the maximum value of the given collection/field. If 'getting' param is
 * set to 'min' this will return the minimum value instead.
 * callback(err, maxValue, doc)
 */
ME.findMax = function (collectionName, fieldName, conditions, callback, getting) {
  conditions = conditions || {};
  getting    = getting    || 'max';

  // Setup query to return one item
  var query = ME.model[collectionName].findOne(conditions);

  // Sort DESC (so we only find the top one)
  var operand = (getting === 'max' ? '-' : (getting === 'min' ? '+' : ''));
  query.sort(operand + fieldName);

  // No callback, return thr query instead
  if (!_.isFunction(callback)) return query;

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
ME.findMin = function (collectionName, fieldName, conditions, callback) {
  return ME.findMax(collectionName, fieldName, conditions, callback, 'min');
};

/*
 * Counts the values in the given field(s). For integer/float fields the count
 * is incremented by the value, for string fields the count is incremented by 1.
 * The callback is passed a hash 'count' containing counters for each of the
 * given fields.
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
  if (_.isString(fields)) fields = [fields];  //ensure fields is an array of strings

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
 * Returns true if the input string is likely to be an ObjectID.
 */
ME.isObjectId = function (input) {
  var regexp = new RegExp("^[0-9a-fA-F]{24}$");
  return regexp.test(input);
};

/*
 * Converts a string to an ObjectID.
 */
ME.toObjectId = function (input) {
  return mongoose.Types.ObjectId(input);
};

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
  if (_.isUndefined(property)) property = '_id';

  var index = null;

  for (var i=0 ; i < arr.length ; i++) {
    var arrItem        = arr[i]
      , isItemObjectId = ME.isObjectId(arrItem)
      , validObjectId  = false
      , value;

    // Passed in an object
    if (!isItemObjectId && _.isObject(arrItem)) {
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