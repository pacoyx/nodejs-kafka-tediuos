const kafka_client = require("../config/kafka.js");
var kafka = require('kafka-node');

exports.enviarOperacionKafka = function (payloads) {

    var Producer = kafka.Producer;
    var producer = new Producer(kafka_client.initKafka());
    console.log('recibimos : ', payloads.length);

    producer.on('ready', function () {
        producer.send(payloads, function (err, data) {
            //console.log('payloads >>', payloads)
            console.log('error send: >>', err)
            console.log('data send: >>', data)
        });
    });

    producer.on('error', function (err) {
        console.log("error de conexion ")
        console.log(err)
    })
}