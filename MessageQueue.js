const EventEmitter = require('events')
const ENQUEUE_EVENT_NAME = 'enqueue';

function MessageQueue() {
    EventEmitter.call(this)
    this.messages = []
}

MessageQueue.prototype = Object.create(EventEmitter.prototype)
MessageQueue.prototype.size = function() {
    return this.messages.length;
}

MessageQueue.prototype.clear = function () {
    this.messages = [];
}

MessageQueue.prototype.enqueue = function (data) {
    // MessageQueue.prototype.emit.call(null, ENQUEUE_EVENT_NAME, data)
    this.emit(ENQUEUE_EVENT_NAME, data)
    this.messages.push(data)
}

module.exports = MessageQueue;
