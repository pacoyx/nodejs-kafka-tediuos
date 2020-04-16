const kafka_client = require("../config/kafka.js");
var kafka = require('kafka-node');
var Offset = kafka.Offset;

getRespuestaKafka = function () {
    const client = kafka_client.initKafka();
    var offset = new Offset(client);
    var options = {
        autoCommit: true,
        fetchMaxWaitMs: 1000,
        fetchMaxBytes: 1024 * 1024
    };

    Consumer = kafka.Consumer;
    consumer = new Consumer(
        client,
        [{
            topic: 'rys_topic'          
        }],
        options

    );
    console.log('paso por aqui');

    consumer.on('message', function (message) {
        console.log('message >>> ', message);

        consumer.commit(function (err, data) {
            console.log('comitiando...', data);
        });


    });

    consumer.on('error', function (message) {
        console.log('error >>> ', message);
    });



    consumer.on('offsetOutOfRange', error => {
        console.log(error)
    })

    // consumer.on('offsetOutOfRange', function (topic) {
    //     topic.maxNum = 2;
    //     offset.fetch([topic], function (err, offsets) {
    //         if (err) {
    //             return console.error(err);
    //         }
    //         var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
    //         consumer.setOffset(topic.topic, topic.partition, min);
    //     });
    // });

}


module.exports.getRespuestaKafka = getRespuestaKafka;