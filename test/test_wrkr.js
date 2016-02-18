/* jshint mocha: true */
'use strict';
var WrkrMongodb = require('../wrkr_mongodb');
var WrkrTestSuite = require('wrkr-tests');


// Initialize our workerInterface
var backend = new WrkrMongodb({
  host:              'localhost',
  name:              'wrkr_unittest',
  dbOpt:             {w: 1},  // Write concern, require ack before callback

  pollInterval:      20,      // default: 500, regular polltimer (waiting for new items)
  pollIntervalBusy:  5,       // default: 5, next-item-polltimer after processing an item

  errorRetryTime:    500,     // default: 5000, on error retry timer
                              //    (not so happy with auto-retrying though)
});
var wrkrTests = new WrkrTestSuite(backend);


// Start testing
describe('backend - start', backendStart);
describe('backend - test required features', backendRequired);
describe('backend - stop', backendStop);


function backendStart() {
  it ('test backend start', function (done) {
    wrkrTests.start(done);
  });
}

function backendRequired() {
  it ('test backend required features', function (done) {
    wrkrTests.required(done);
  });
}

function backendStop() {
  it ('test backend stop', function (done) {
    wrkrTests.stop(done);
  });
}
