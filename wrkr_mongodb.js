'use strict';
var mongoose = require('mongoose');
var debug = require('debug')('wrkr:mongodb');


/********************************************************************
  Schema's & Models
 ********************************************************************/

var subscriptionSchema = {
  eventName:  {type: String, unique: true},
  queues:     []
};


var qItemsSchema = {
  queue:      {type: String, index: true},
  created:    {type: Date, index: true, default: Date.now},

  name:       {type: String},
  tid:        {type: String, index: true},
  payload:    {type: mongoose.Schema.Types.Mixed},

  when:       {type: Date, index: true, sparse: true},
  done:       {type: Date, index: true, sparse: true}
};


/********************************************************************
  WrkrMongodb
 ********************************************************************/

var optDefaults = {
  host:              'localhost',
  port:              27017,
  name:              'wrkr' + (process.env.NODE_ENV ? '_'+process.env.NODE_ENV : ''),
  user:              '',
  pswd:              '',
  dbOpt:             {w: 1},  // Write concern

  pollInterval:      500,     // regular polling time (waiting for new items)
  pollIntervalBusy:  5        // quick-fetch-next-polltimer after processing an item
};


function WrkrMongodb(opt) {
  opt = opt || {};
  this.opt = {};
  this.wrkr = null;
  this.db = null;

  this.listenQueues = {};       // queues the server shpuld listen on
  this.pollTimer = null;        // the poll-timer, so that we can stop it;

  // Mongoose Models
  this.Subscription = null;
  this.QueueItems = null;

  // Set (default) options
  Object.keys(optDefaults).forEach( (k) => {
    this.opt[k] = (opt[k] === undefined) ? optDefaults[k] : opt[k];
  });
}


/********************************************************************
  Starting and Stopping
 ********************************************************************/

WrkrMongodb.prototype.start = function start(wrkr, opt, done) {
  this.wrkr = wrkr;

  // Create mongodb connectionString
  let dbUserPswd = '';
  if (this.opt.user && this.opt.pswd) {
    dbUserPswd = `${this.opt.user}:${this.opt.pswd}@`;
  }
  let dbConnectStr =
    `mongodb://${dbUserPswd}${this.opt.host}:${this.opt.port}/${this.opt.name}`;

  // Connect
  debug('starting wrkr_mongodb :'+dbConnectStr);
  this.db = mongoose.createConnection();
  this.db.open(dbConnectStr, this.opt.dbOpt, (err) => {
    if (err) return done(err);

    // Make models
    this.Subscription = this.db.model('wrkr_subscriptions',
      new mongoose.Schema(subscriptionSchema)
    );
    this.QueueItems = this.db.model('wrkr_qitems',
      new mongoose.Schema(qItemsSchema)
    );

    debug('connected to mongodb');
    return done();
  });
};


WrkrMongodb.prototype.stop = function stop(done) {
  debug('stopping wrkr_mongodb');

  // Stop pollTimer (if started)
  //    this.pollTimer = null indicates that we are stopping,
  //    poll() should not set a new timeout
  if (this.pollTimer) {
    clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  this.db.close(function (err) {
    debug('closed database, stop done, err: ' + (err ? err.toString() : '<none>'));
    return done(err || null);
  });
};


/********************************************************************
  Subscriptions functions
 ********************************************************************/

WrkrMongodb.prototype.subscribe = function subscribe(queueName, eventName, done) {
  debug('subscribe', queueName, eventName);
  this.listenQueues[queueName] = true;

  var find = {eventName: eventName};
  var update = {
    $addToSet: {queues: queueName}
  };
  // debug('subscribe', find, update);
  this.Subscription.findOneAndUpdate(find, update, {upsert: true}, function (err) {
    return done(err || null);
  });
};


WrkrMongodb.prototype.unsubscribe = function unsubscribe(queueName, eventName, done) {
  debug('unsubscribe', queueName, eventName);
  this.listenQueues[queueName] = true;

  var find = {eventName: eventName};
  var update = {
    $pull: {queues: queueName}
  };
  // debug('unsubscribe', find, update);
  this.Subscription.findOneAndUpdate(find, update, {upsert: true}, function (err) {
    return done(err || null);
  });
};


WrkrMongodb.prototype.subscriptions = function subscriptions(eventName, done) {
  var find = {eventName: eventName};
  this.Subscription.findOne(find, function (err, subscription) {
    var result = subscription ? subscription.queues : [];
    debug(`subscriptions for ${eventName}`, result);
    return done(err, result);
  });
};


/********************************************************************
  Publishing functions
 ********************************************************************/


WrkrMongodb.prototype.publish = function publish(events, done) {
  debug('publish', events);
  return this.QueueItems.collection.insertMany(events, done);
};


/********************************************************************
  Event andling
 ********************************************************************/

WrkrMongodb.prototype.getQueueItems = function getQueueItems(filter, done) {
  var mongoFilter = Object.create(filter);
  mongoFilter.when = {$exists: true};
  this.QueueItems.find(mongoFilter, {}, {sort: {created: 1}}, function (err, qitems) {
    return done(err || null, qitems);
  });
};


// Delete specific qitem
WrkrMongodb.prototype.deleteQueueItems = function deleteQueueItems(spec, done) {
  debug('deleteQueueItems', spec);
  this.QueueItems.remove(spec, function (err) {
    return done(err || null);
  });
};


/********************************************************************
  Listen functions
 ********************************************************************/

WrkrMongodb.prototype.listen = function listen(done) {
  var self = this;
  var listenQueues = Object.keys(self.listenQueues);

  debug('listen - start polling - polltimer: started', self.listenQueues);

  self.pollTimer = Date.now();
  setImmediate(function () {
    poll();
  });
  return done(null);

  function updatePollTimer(timeoutMs) {
    if (self.pollTimer === null) {
      return debug('listen - updatePollTimer: detected stoppage');
    }
    self.pollTimer = setTimeout(poll, timeoutMs);
  }

  function poll() {
    // First we have to check if there's work to be done
    var searchSpec = {
      when:  {$lte: new Date()},
      queue: {$in: listenQueues}
    };
    var options = {
      sort: {created: 1}
    };
    var updateSpec = {
      $unset: {when: 1},
      $set: {done: new Date()}
    };
    // debug('polling', searchSpec, updateSpec, sort);
    self.QueueItems.findOneAndUpdate(searchSpec, updateSpec, options, (err, event) => {
      if (err) throw err;

      // No more events to process, go to regular polling interval
      if (!event) return updatePollTimer(self.opt.pollInterval);

      // Prepare event to dispatch to Wrkr
      var eventObj = event.toObject();

      debug('dispatching event to Wkrk', eventObj);
      self.wrkr._dispatch(eventObj, (err) => {
        // Error handling and archiving should have been handles by Wrkr
        if (err) {
          debug('ignoring error '+err.message);
        }
        // Just start a new polltimer
        return updatePollTimer(self.opt.pollIntervalBusy);
      });
    });
  }
};


/********************************************************************
  Exports
 ********************************************************************/

module.exports = WrkrMongodb;
