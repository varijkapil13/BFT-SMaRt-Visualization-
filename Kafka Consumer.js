//=======================================Kafka consumer definition
var mongo = require('mongodb').MongoClient;
var format = require('util').format;
var collection = null;
var kafka = require("kafka-node");
var Consumer = kafka.Consumer;
var Client = kafka.Client;

var client = new Client('192.168.12.57:2181');
var topics = [
	{
		topic: "systemstat",
		partition: 0
	},
	{
		topic: "replyAtClientReceived",
		partition: 0
	},
	{
		topic: "orderedRequestFromClientManagerAtServer",
		partition: 0
	},
	{
		topic: "unorderedRequestFromClientAtServer",
		partition: 0
	},
	{
		topic: "orderedRequestFromClientAtClientManager",
		partition: 0
	},
	{
		topic: "consensusMessage",
		partition: 0
	},
	{
		topic: "sendMessage",
		partition: 0
	}
];
var options = {
	autoCommit: false,
	fetchMaxWaitMs: 1000,
	fetchMaxBytes: 1024 * 1024
};

var consumer = new Consumer(client, topics, options);

//=======================================How node server handles client
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.get('/', function (req, res) {
	res.sendFile(__dirname + '/index.html')
});

http.listen(8080, function () {
	console.log('listening on *:8080');
});

//=======================================Main

connectMongo("testCollection");

function connectMongo(Collection) { //connectMongo() definition
	mongo.connect('mongodb://192.168.12.62:27017/cloud', function (err, db) { //db to connect to
		if (err) {
			throw err;
		} else {
			console.log("successfully connected to the database");
		}
		collection = db.collection(Collection);
	});
}
// get the data fom mongo db
io.on('connection', function (client) {
	console.log('a user connected');
	consumer.on('message', function (msg) {

		if (msg.topic != "systemstat") {
			collection.insert(JSON.parse(msg.value), function () {});
		}

		if (msg.topic == "sendMessage") {
			var sentmessages = collection.find({
				$and: [{
					$or: [{
						"messagetype": "consensus"
					}, {
						"messagetype": "client reply"
					}]
				}, {
					"data": "sendmessage"
				}]
			}).toArray(function (err, result) {
				client.emit("messages-sent", result);
			});
			var receivedmessages = collection.find({
				$or: [{
					"data": "consensus"
				}, {
					"data": "orderedrequestatserver"
				}, {
					"data": "unorderedrquestfromclient"
				}]
			}).toArray(function (err, result) {
				client.emit("messages-rec", result);
			});
		} else if (msg.topic == "consensusMessage" || msg.topic == "unorderedRequestFromClientAtServer" || msg.topic == "orderedRequestFromClientManagerAtServer") {

			client.emit("new-message", msg.value);

		} else if (msg.topic == "systemstat") {
			client.emit(msg.topic, msg.value);
		} else if (msg.topic == "replyAtClientReceived") {
			client.emit("new-message", msg.value);
		}

	});

	client.on('disconnect', function () {
		console.log('user disconnected');
	});
});

consumer.on('error', function (err) {
	console.log('error', err);

});
