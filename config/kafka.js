
var kafka = require('kafka-node');
const dotenv = require('dotenv');
dotenv.config();

function initKafka(){
    return new kafka.KafkaClient({kafkaHost: process.env.kafka_connection});
}

module.exports = {
    initKafka
}