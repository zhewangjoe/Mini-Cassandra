'use strict';

var fetchConfig = require('zero-config');
var fs = require('fs');
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

    // Example data structure on application
    self.exampleDb = {};
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
    console.log("find successor");

    find_predecessor(body.key, function (pre) {
        RPCSuccessor(pre, function (successor) {
            cb(null, {
                ok: true,
                body: successor
            });
        })
    });
};

Application.successor = function successor(app, req, head, body, cb) {
    cb(null, {
        ok: true,
        body: GLOBAL.successor
    });
};

Application.predecessor = function predecessor(app, req, head, body, cb) {
    cb(null, {
        ok: true,
        body: GLOBAL.predecessor
    });
};

Application.closed_preceding_finger = function closed_preceding_finger(app, req, head, body, cb) {
    for (var i = GLOBAL.m; i > 0; i--) {
        if (isBetween(GLOBAL.finger[i].node, GLOBAL.serviceName, body.key)) {
            cb(null, {
                ok: true,
                body: GLOBAL.finger[i].node
            });
            return;
        }
    }
    cb(null, {
        ok: true,
        body: GLOBAL.serviceName
    });
};

Application.find_predecessor = function find_successor (key, cb) {
    console.log("find predecessor");

    if ( !isBetweenRightIncluded(key, GLOBAL.serviceName, GLOBAL.successor) ) {
        RPCClosedPrecedingFinger(GLOBAL.serviceName, key, RPCClosedPrecedingFingerCallBack);
    } else {
        cb(GLOBAL.serviceName);
    }
};

Application.RPCSuccessor = function RPCSuccessor (remote, cb) {
    var keyChan = this.clients.hyperbahnClient.getClientChannel({
        serviceName: remote
    });

    var keyThrift = this.clients.rootChannel.TChannelAsThrift({
        source: thriftFile,
        channel: keyChan
    });

    keyThrift.request({
        serviceName: remote,
        timeout: 100,
        hasNoParent: true
    }).send('MyService::successor_v1', null, null, function onResponse(err, resp) {
        if (err) {
            return console.error('got an error', {
                error: err
            });
        }

        cb(resp.body);
    });
};

Application.RPCClosedPrecedingFinger = function RPCClosedPrecedingFinger (remote, key, cb) {
    var keyChan = this.clients.hyperbahnClient.getClientChannel({
        serviceName: remote
    });

    var keyThrift = this.clients.rootChannel.TChannelAsThrift({
        source: thriftFile,
        channel: keyChan
    });

    keyThrift.request({
        serviceName: remote,
        timeout: 100,
        hasNoParent: true
    }).send('MyService::closed_preceding_finger_v1', null, {
        key: key
    }, function onResponse(err, resp) {
        if (err) {
            return console.error('got an error', {
                error: err
            });
        }

        cb(resp.body);
    });
};

Application.RPCClosedPrecedingFingerCallBack = function RPCClosedPrecedingFingerCallBack (pre) {
    RPCSuccessor(pre, function (successor) {
        if (!isBetweenRightIncluded(key, pre, successor)) {
            RPCClosedPrecedingFinger(pre, key, RPCClosedPrecedingFingerCallBack);
        } else {
            cb(pre);
        }
    })
}

Application.isBetween = function isBetween (node, node1, node2) {
    var key = parseInt(node);
    var key1 = parseInt(node1);
    var key2 = parseInt(node2);

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
    var key = parseInt(node);
    var key1 = parseInt(node1);
    var key2 = parseInt(node2);

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
