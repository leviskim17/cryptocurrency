var kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    client = new kafka.Client(),
    consumer = new HighLevelConsumer(
        client,
        [
            { topic: 'price' }
        ],
        {
            groupId: 'my-group'
        }
    );

consumer.on('message', function (message) {
    console.log(message.value);
});
