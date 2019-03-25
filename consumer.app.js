
const kafka = require('kafka-node');
const bp = require('body-parser');
const config = require('./config');
var avro = require('avsc');

try {
  const Consumer = kafka.HighLevelConsumer;
  const client = new kafka.Client(config.kafka_server);
  let consumer = new Consumer(
    client,
    [{ topic: config.kafka_topic, partition: 0 }],
    {
      autoCommit: false,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      fromOffset: 'latest',
    }
  );
  consumer.on('message', async function(message) {
    console.log(
      'kafka-> ',
      message
    );
  })
  consumer.on('error', function(err) {
    console.log('error', err);
  });
}
catch(e) {
  console.log(e);
}
