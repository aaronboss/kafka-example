'use strict';

const kafka = require('kafka-node');
const config = require('./config');
const { runSinkConnector, ConverterFactory } = require("sequelize-kafka-connect");
var avro = require('avsc')

var KeyedMessage = kafka.KeyedMessage;
var client = new kafka.KafkaClient();
// const producer = new kafka.Producer(client);
// var argv = require('optimist').argv;
var topic = config.kafka_topic;
var producer = new kafka.Producer(client, { requireAcks: 1 });

  var schema = avro.parse({
    "name":"myrecord",
    "type":"record",
    "fields":[{
      "name":"id",
      "type":"int"
    },{
      "name":"quantity",
      "type": "int"
    },{
      "name":"product",
      "type": "string"
    },{
      "name":"price",
      "type": "float"
    }]
  })

  var obj = {"id": 999, "product": "foo", "quantity": 100, "price": 50}
  var data = schema.toBuffer(obj)


producer.on('ready', function () {
  var message = 'a message';
  var keyedMessage = new KeyedMessage('data', data);

  producer.send([{ topic: topic, partition:0, messages: [message, keyedMessage] }], function (
    err,
    result
  ) {
    console.log(err || result);
    process.exit();
  });
});

producer.on('error', function (err) {
  console.log('error', err);
});



// const kafka = require('kafka-node');
// const bp = require('body-parser');
// const config = require('./config');
// var avro = require('avsc')
// const schemaData = require('./schema.json');
//
// try {
//   const Producer = kafka.Producer;
//   const client = new kafka.Client(config.zookeeper_server);
//   const producer = new Producer(client);
//   const KeyedMessage = kafka.KeyedMessage;
//
//   producer.connect();
//
//   // var schema = avro.parse(schemaData)
//   var schema = avro.parse({
//     "name":"myrecord",
//     "type":"record",
//     "fields":[{
//       "name":"id",
//       "type":"int"
//     },{
//       "name":"quantity",
//       "type": "int"
//     },{
//       "name":"product",
//       "type": "string"
//     },{
//       "name":"price",
//       "type": "float"
//     }]
//   })
//
//   // var schema = avro.parse({
//   //   "type":"record",
//   //   "name":"myrecord",
//   //   "fields":[{
//   //     "name":"f1",
//   //     "type":"string"
//   //   }]})
//
//   // const resolver = lightType.createResolver(heavyType);
//   var obj = {"id": 999, "product": "foo", "quantity": 100, "price": 50}
//   var data = schema.toBuffer(obj)
//
//   let payloads = [
//     {
//       topic: config.kafka_topic,
//       messages: 'test'
//     },
//   ];
//
//   producer.on('ready', async function() {
//     console.log('in the producer')
//     const keyedMessage = new KeyedMessage('id', Buffer.from(data.buffer))
//     // payloads[0].messages = keyedMessage;
//     console.log('payload', keyedMessage);
//     let push_status = producer.send(payloads, (err, data) => {
//       if (err) {
//         console.log('[kafka-producer -> '+config.kafka_topic +']: broker update failed', err);
//       } else {
//         console.log('kafka data:', data, producer);
//       }
//     });
//   });
//
//   producer.on('error', function(err) {
//     console.log(err);
//     throw err;
//   });
// }
// catch(e) {
//   console.log(e);
// }
