suite('Firehose', () => {
  let _ = require('lodash');
  let assert = require('assert');
  let monitoring = require('../');
  let AWS = require('aws-sdk-mock');
  let testing = require('taskcluster-lib-testing');

  let streamName = 'mocked-stream';
  let monitor = null;
  let records = {};

  setup(async () => {
    AWS.mock('Firehose', 'describeDeliveryStream', (params, callback) => {
      records[params.DeliveryStreamName] = [];
      callback(null, {DeliveryStreamDescription: {DeliveryStreamStatus: 'ACTIVE'}});
    });
    AWS.mock('Firehose', 'putRecordBatch', (params, callback) => {
      if (params.Records.length > 500) {
        throw new Error('Too many records written at once!');
      }
      let size = params.Records.reduce((acc, line) => {
        let length = Buffer.byteLength(line.Data.trim(), 'utf-8');
        if (length > 1000 * 1000) {
          throw new Error('Individual record too large!');
        }
        return acc + length;
      }, 0);
      if (size > 4 * 1000 * 1000) {
        throw new Error('Total group of records too large!');
      }
      let DeliveryStreamName = params.DeliveryStreamName;
      records[DeliveryStreamName] = records[DeliveryStreamName].concat(params.Records.map(x => x.Data.trim()));
      callback(null, {FailedPutCount: 0});
    });

    monitor = await monitoring({
      project: 'tc-lib-monitor',
      credentials: {clientId: 'test-client', accessToken: 'test'},
      patchGlobal: false,
      reportStatsumErrors: false,
      reportFirehoseErrors: false,
      resourceInterval: 1,
      aws: {credentials: {accessKeyId: 'foo', secretAccessKey: 'bar'}, region: 'us-east-1'},
      streamName,
    });
  });

  teardown(() => {
    AWS.restore();
    records = {};
  });

  test('should write logs on explicit flush', async function () {
    let subject = {test: 123};
    monitor.firehose.log(subject);
    await monitor.flush();
    assert.equal(records[streamName].length, 1);
    assert.deepEqual(records[streamName].map(JSON.parse)[0], subject);
  });

  test('should write logs on 500 records', async function () {
    let subjects = _.range(5302).map(i => ({foo: i}));
    subjects.forEach(subject => monitor.firehose.log(subject));
    await monitor.firehose.close();
    assert.equal(records[streamName].length, 5302);
    assert.deepEqual(records[streamName].map(JSON.parse), subjects);
  });

  test('should write logs on too many bytes', async function () {
    // This should get the records to be over 4MB in total
    let subjects = _.range(250).map(i => ({foo: i, bar: _.repeat('#', 16000)}));
    subjects.forEach(subject => monitor.firehose.log(subject));
    await monitor.firehose.close();
    assert.equal(records[streamName].length, 250);
    assert.deepEqual(records[streamName].map(JSON.parse), subjects);
  });

  test('should write logs on timeout', async function () {
    let subject = {test: 1000, timerthing: true};
    monitor.firehose.log(subject);
    await testing.sleep(2000); // Should be flushing every second during testing
    assert.equal(records[streamName].length, 1);
    assert.deepEqual(records[streamName].map(JSON.parse)[0], subject);
  });

  test('should write previously failed records', async function () {
    let tries = 0;
    AWS.restore('Firehose', 'putRecordBatch');
    AWS.mock('Firehose', 'putRecordBatch', (params, callback) => {
      let DeliveryStreamName = params.DeliveryStreamName;
      let success = params.Records.splice(0, tries++);
      records[DeliveryStreamName] = records[DeliveryStreamName].concat(success.map(x => x.Data.trim()));
      let responses = success.map(() => ({RecordId: 'fake'}));
      responses = responses.concat(params.Records.map(() => ({ErrorCode: '500', ErrorMessage: 'oh no!'})));
      callback(null, {
        FailedPutCount: params.Records.length,
        RequestResponses: responses,
      });
    });
    let subjects = [
      {test: 1000},
      {test: 2000},
      {test: 3000},
      {test: 4000},
      {test: 5000},
    ];
    subjects.forEach(subject => monitor.firehose.log(subject));
    await monitor.firehose.close();
    assert.equal(records[streamName].length, 5);
    assert.deepEqual(records[streamName].map(JSON.parse), subjects);
  });

  test('should eventually stop trying to resubmit', async function () {
    AWS.restore('Firehose', 'putRecordBatch');
    AWS.mock('Firehose', 'putRecordBatch', (params, callback) => {
      callback(null, {
        FailedPutCount: 1,
        RequestResponses: [{ErrorCode: '500', ErrorMessage: 'oh no!'}],
      });
    });
    let closed = false;
    monitor.firehose.log({test: 'foobar'});
    monitor.firehose.close().then(() => {closed = true;});
    await testing.sleep(1000);
    assert(closed, 'Failed to reject permanently failing submission.');
  });

  test('should resubmit all on error', async function () {
    let tried = false;
    AWS.restore('Firehose', 'putRecordBatch');
    AWS.mock('Firehose', 'putRecordBatch', (params, callback) => {
      if (!tried) {
        tried = true;
        return callback({code: 500, message: 'uh oh!'}, null);
      }
      let DeliveryStreamName = params.DeliveryStreamName;
      records[DeliveryStreamName] = records[DeliveryStreamName].concat(params.Records.map(x => x.Data.trim()));
      callback(null, {FailedPutCount: 0});
    });
    monitor.firehose.log({test: 'foobar'});
    monitor.firehose.log({test: 'foobar2'});
    await monitor.firehose.close();
    assert.equal(records[streamName].length, 2);
  });
});
