'use strict';

var fetchConfig = require('zero-config');
var fs = require('fs');
var path = require('path');
var thriftFile = fs.readFileSync(
    path.join(__dirname, 'thrift', 'service.thrift'), 'utf8'
);

var ApplicationClients = require('./clients.js');

module.exports = Application;

function Application(options) {
    if (!(this instanceof Application)) {
        return new Application(options);
    }

    var self = this;
    options = options || {};

    self.config = fetchConfig(__dirname, {
        dcValue: 'todo',
        seed: options.seedConfig,
        loose: false
    });

    self.config.set('serviceName', options);
    self.key = options;
    self.finger = [];
    self.m = 3;
    self.next = self.m;
    self.clients = ApplicationClients(self.config, {
        logger: options.logger,
        statsd: options.statsd
    });

    var channel = self.clients.appChannel;
    var thrift = self.clients.tchannelThrift;

    thrift.register(
        channel, 'MyService::health_v1', self, Application.health
    );

    // TODO remove example endpoints
    thrift.register(
        channel, 'MyService::get_v1', self, Application.get
    );
    thrift.register(
        channel, 'MyService::put_v1', self, Application.put
    );

    thrift.register(
        channel, 'MyService::find_successor_v1', self, Application.find_successor
    );

    thrift.register(
        channel, 'MyService::predecessor_v1', self, Application.predecessor
    );

    thrift.register(
        channel, 'MyService::notify_v1', self, Application.notify
    );

    // Example data structure on application
    self.exampleDb = {};

    self.init = function () {
        if (self.key === "node0") {
            self.predecessor = "nil";
            self.successor = self.key;
        } else {
            self.predecessor = "nil";
            self.RPCFindSuccessor("node0", self.key, function (successor) {
                self.successor = successor;
            });
        }
        setInterval(self.stabilize, 500);
        setInterval(self.fix_fingers, 300);
        setInterval(self.check_predecessor, 3000);
        setInterval(self.check_successor, 5000);
    };

    self.RPCFindSuccessor = function (remote, key, cb) {
        var keyChan = self.clients.hyperbahnSendClient.getClientChannel({
            serviceName: remote
        });

        var keyThrift = self.clients.sendChannel.TChannelAsThrift({
            source: thriftFile,
            channel: keyChan
        });

        keyThrift.request({
            serviceName: remote,
            timeout: 100,
            hasNoParent: true
        }).send('MyService::find_successor_v1', null, {
            key: key
        }, function onResponse(err, resp) {
            if (err) {
                return err;
            }
            cb(resp.body);
        });
    };

    self.closed_preceding_finger = function (id) {
        for (var i = self.m; i > 0; i--) {
            if (Application.isBetween(self.finger[i], self.key, id)) {
                return self.finger[i];
            }
        }
        return self.key;
    };

    self.stabilize = function () {
        console.log("pre: " + self.predecessor);
        console.log("succ: " + self.successor);
        console.log(self.finger);
        self.RPCPredecessor(self.successor, function (predecessor) {
            if (Application.isBetween(predecessor, self.key, self.successor)) {
                self.successor = predecessor;
            }
            self.RPCNotify(self.successor, self.key);
        });
    };

    self.RPCPredecessor = function (remote, cb) {

        var keyChan = self.clients.hyperbahnSendClient.getClientChannel({
            serviceName: remote
        });

        var keyThrift = self.clients.sendChannel.TChannelAsThrift({
            source: thriftFile,
            channel: keyChan
        });

        keyThrift.request({
            serviceName: remote,
            timeout: 1000,
            hasNoParent: true
        }).send('MyService::predecessor_v1', null, null, function onResponse(err, resp) {
            if (err) {
                return err;
            }

            cb(resp.body);
        });
    };

    self.RPCNotify = function (remote, key) {

        var keyChan = self.clients.hyperbahnSendClient.getClientChannel({
            serviceName: remote
        });

        var keyThrift = self.clients.sendChannel.TChannelAsThrift({
            source: thriftFile,
            channel: keyChan
        });

        keyThrift.request({
            serviceName: remote,
            timeout: 1000,
            hasNoParent: true
        }).send('MyService::notify_v1', null, {
            key: key
        }, function onResponse(err, resp) {
            if (err) {
                return err;
            }
        });
    };

    self.fix_fingers = function () {
        self.next += 1;
        if (self.next > self.m) {
            self.next = 1;
        }
        var nextKey = self.key;
        nextKey = parseInt(nextKey.replace ( /[^\d.]/g, '' )) + Math.pow(2, self.next - 1);
        nextKey = nextKey % Math.pow(2, self.m);
        nextKey = "node" + nextKey;
        self.RPCFindSuccessor(self.key, nextKey, function (successor) {
            self.finger[self.next] = successor;
        });
    };

    self.check_predecessor = function () {
        var keyChan = self.clients.hyperbahnSendClient.getClientChannel({
            serviceName: self.predecessor
        });

        var keyThrift = self.clients.sendChannel.TChannelAsThrift({
            source: thriftFile,
            channel: keyChan
        });

        keyThrift.request({
            serviceName: self.predecessor,
            timeout: 1000,
            hasNoParent: true
        }).send('MyService::predecessor_v1', null, null, function onResponse(err, resp) {
            if (err) {
                self.predecessor = "nil";
            }
        });
    };

    self.check_successor = function () {
        var keyChan = self.clients.hyperbahnSendClient.getClientChannel({
            serviceName: self.successor
        });

        var keyThrift = self.clients.sendChannel.TChannelAsThrift({
            source: thriftFile,
            channel: keyChan
        });

        keyThrift.request({
            serviceName: self.successor,
            timeout: 1000,
            hasNoParent: true
        }).send('MyService::predecessor_v1', null, null, function onResponse(err, resp) {
            if (err) {
                self.successor = self.key;
            }
        });
    };
}

Application.prototype.bootstrap = function bootstrap(cb) {
    var self = this;

    self.clients.bootstrap(cb);
};

Application.prototype.destroy = function destroy() {
    var self = this;

    self.clients.destroy();
};

Application.health = function health(app, req, head, body, cb) {
    cb(null, {
        ok: true,
        body: {
            message: 'ok'
        }
    });
};

// TODO remove me
Application.get = function get(app, req, head, body, cb) {
    if (!(body.key in app.exampleDb)) {
        return cb(null, {
            ok: false,
            body: new Error('no such key ' + body.key),
            typeName: 'noKey'
        });
    }

    var value = app.exampleDb[body.key];

    cb(null, {
        ok: true,
        body: value
    });
};

// TODO remove me
Application.put = function put(app, req, head, body, cb) {
    app.exampleDb[body.key] = body.value;

    cb(null, {
        ok: true,
        body: null
    });
};

Application.find_successor = function find_successor(app, req, head, body, cb) {

    if (Application.isBetweenRightIncluded(body.key, app.key, app.successor)) {
        cb(null, {
            ok: true,
            body: app.successor
        });
    } else {
        var pre = app.closed_preceding_finger(body.key);

        app.RPCFindSuccessor(pre, body.key, function (successor) {
            cb(null, {
                ok: true,
                body: successor
            });
        });
    }
};

Application.notify = function notify(app, req, head, body, cb) {
    if (app.predecessor === "nil" || Application.isBetween(body.key, app.predecessor, app.key)) {
        app.predecessor = body.key;
    }
    cb(null, {
        ok: true,
        body: null
    });
};

Application.predecessor = function predecessor(app, req, head, body, cb) {
    cb(null, {
        ok: true,
        body: app.predecessor
    });
};

Application.isBetween = function isBetween (node, node1, node2) {
    if (node === undefined || node === "nil") {
        return false;
    }
    var key = parseInt(node.replace ( /[^\d.]/g, '' ));
    var key1 = parseInt(node1.replace ( /[^\d.]/g, '' ));
    var key2 = parseInt(node2.replace ( /[^\d.]/g, '' ));

    if (key1 < key2) {
        if (key > key1 && key < key2){
            return true;
        }
    } else {
        if (key1 > key2) {
            if (key > key1 ) {
                return true;
            } else {
                if (key < key2) {
                    return true;
                }
            }
        } else {
            if ( key != key2 ) {
                return true;
            }
        }
    }

    return false;
};

Application.isBetweenRightIncluded = function isBetweenRightIncluded (node, node1, node2) {
    if (node === undefined || node === "nil") {
        return false;
    }
    var key = parseInt(node.replace ( /[^\d.]/g, '' ));
    var key1 = parseInt(node1.replace ( /[^\d.]/g, '' ));
    var key2 = parseInt(node2.replace ( /[^\d.]/g, '' ));

    if (key1 < key2) {
        if (key > key1 && key <= key2){
            return true;
        }
    } else {
        if (key1 > key2) {
            if (key > key1 ) {
                return true;
            } else {
                if (key <= key2) {
                    return true;
                }
            }
        } else {
            return true;
        }
    }

    return false;
};
