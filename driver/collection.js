const MongooseCollection = require('mongoose/lib/collection');
const _ = require('lodash');
const ObjectId = require('bson').ObjectId;
const sift = require('sift');

const levelup = require('levelup');
const leveldown = require('leveldown');
const {compileSort, compileDocumentSelector} = require('minimongo/lib/selector');
const multilevel = require('multilevel');
const net = require('net');
const path = require('path');
const fs = require('fs');
const q = require('q');
const jsonfn = require('./jsonfn')
let port = 3001;

class TingoCollection extends MongooseCollection {
    constructor() {
        super(...arguments);
        if (this.conn.uri) this.init();
    }

    afterInit(cb) {
        this.loaded = false;
        this.idx = [];
        this.indexDb.createReadStream()
            .on('data', data => {
                this.idx.push(jsonfn.parse(data.value));
            })
            .on('end', () => {
                this.loaded = true;
                super.onOpen();
                if (cb) cb();
            })
    }

    rebuildIndex() {
        return new Promise((resolve, reject) => {
            const docs = [];
            this.dataDb.createReadStream()
                .on('data', data => {
                    docs.push(jsonfn.parse(data.value));
                })
                .on('end', () => {
                    Promise.all(this.idx.map(idx => q.ninvoke(this.indexDb, 'del', idx._id))).then(() => {
                        this.idx = [];
                        for (const doc of docs) {
                            this.idx.push(this.getIndex(doc));
                            this.indexDb.put(doc._id, jsonfn.stringify(this.getIndex(doc)), () => null);
                        }
                        resolve();
                    })
                })
        })
    }

    init() {
        this.initBegin = true;
        this.indexes = [];
        const base = `${this.conn.uri.split('//')[1]}/${this.name}`;
        const makeSockPath = (isIndex = false) => {
            if (!fs.existsSync(path.join(base, `../../z_sock`))) fs.mkdirSync(path.join(base, `../../z_sock`));
            return process.platform === 'win32' ?
                '\\\\.\\pipe\\level-party\\' + path.resolve(base) :
                path.join(base, `../../z_sock/${this.name + (isIndex ? '_index' : '')}`);
        }

        const createSock = (isIndex = false) => {
            const _path = makeSockPath(isIndex);
            fs.writeFileSync(_path, port, 'utf-8');
            return port;
        }

        const readSock = (isIndex = false) => {
            const _path = makeSockPath(isIndex);
            return fs.readFileSync(_path, 'utf-8');
        }

        const _init = () => {
            this.dataDb = levelup(leveldown(`${base}`), {compression: false});
            this.indexDb = levelup(leveldown(`${base}_index`), {compression: false});
            this.afterInit();
        }

        if (this.conn.uri.split('//')[0].includes('server')) {
            if (fs.existsSync(makeSockPath(false))) fs.unlinkSync(makeSockPath(false))
            if (fs.existsSync(makeSockPath(true))) fs.unlinkSync(makeSockPath(true))

            net.createServer(con => {
                con.pipe(multilevel.server(this.dataDb)).pipe(con);
                con.on('error', console.log);
            }).listen(createSock(false));
            port++;
            net.createServer(con => {
                con.pipe(multilevel.server(this.indexDb)).pipe(con)
                con.on('error', console.log);
            }).listen(createSock(true));
            port++;
            _init();
        } else if (this.conn.uri.split('//')[0].includes('client')) {
            this.indexDb = multilevel.client();
            const con = net.connect(readSock(true));
            con.pipe(this.indexDb.createRpcStream()).pipe(con);

            con.on('connect', () => {
                this.dataDb = multilevel.client();
                const con2 = net.connect(readSock(false));
                con2.pipe(this.dataDb.createRpcStream()).pipe(con2);

                con2.on('connect', () => {
                    this.afterInit();
                })
            });
        } else {
            _init();
        }
    }

    onOpen() {
        if (!this.initBegin) this.init();
    }

    getIndex(doc) {
        return _.pick(doc, this.indexes.concat(['_id']));
    }

    insert(doc, opt, cb) {
        if (!this.loaded) return this.queue.push(['insert', arguments]);
        normalize(doc);
        this.idx.push(this.getIndex(doc));
        this.indexDb.put(doc._id, jsonfn.stringify(this.getIndex(doc)), () => null);
        this.dataDb.put(doc._id, jsonfn.stringify(doc), cb);
    }

    drop(cb) {
        cb();
    }

    ensureIndex(obj, options, cb) {
        const fieldNames = _.map(obj, (v, k) => k);
        this.indexes.push(...fieldNames);
    }

    createIndex(obj, options) {
        const fieldNames = _.map(obj, (v, k) => k);
        this.indexes.push(...fieldNames);
    }

    findOne(query, opts, _cb) {
        if (!this.loaded) return this.queue.push(['findOne', arguments]);

        if (opts) delete opts.fields;
        normalize(query);
        let [key] = processFind(this.idx, query, opts).map(doc => doc._id);
        if (key) {
            this.dataDb.get(key, (err, doc) => {
                _cb(err, jsonfn.parse(doc));
            });
        } else {
            _cb(null, null);
        }
    }

    findAsync(query, opts) {
        if (!this.loaded) return this.queue.push(['findAsync', arguments]);
        return new Promise((resolve, reject) => {
            this.find(query, opts, (err, cursor) => {
                cursor.toArray((err, docs) => {
                    resolve(docs);
                })
            })
        })
    }

    find(query, opts, _cb) {
        if (!this.loaded) return this.queue.push(['find', arguments]);
        if (opts) delete opts.fields;
        normalize(query);
        let keys = processFind(this.idx, query, opts).map(doc => doc._id);
        //let docs = [];
        const cb = function (err, docs) {
            _cb(err, {
                toArray: cb2 => {
                    cb2(null, docs);
                }
            })
        }

        if (!_.isEmpty(keys)) {
            Promise.all(keys.map(_id => q.ninvoke(this.dataDb, 'get', _id)))
                .then(docs => {
                    docs = docs.map(doc => jsonfn.parse(doc));
                    cb(null, docs)
                })
                .catch(err => cb(err))
        } else {
            cb(null, []);
        }
    }

    count(query, opts, cb) {
        if (!this.loaded) return this.queue.push(['count', arguments]);
        if (opts) delete opts.fields;
        normalize(query);
        let count = processFind(this.idx, query, opts).length;
        cb(null, count);
    }

    modifyById(doc) {
        normalize(doc);
        this.idx[_.findKey(this.idx, id => id._id === doc._id)] = this.getIndex(doc);
        q.ninvoke(this.dataDb, 'put', doc._id, jsonfn.stringify(doc)).then();
        q.ninvoke(this.indexDb, 'put', doc._id, jsonfn.stringify(this.getIndex(doc))).then();
    }

    findAndModify(query, sort, update, opts = {}, cb) {
        if (!this.loaded) return this.queue.push(['findAndModify', arguments]);
        normalize(update);
        if (update.$setOnInsert) delete update.$setOnInsert;

        this.find(query, opts, (err, _docs) => {
            _docs.toArray((err, docs) => {
                const cmd = [];
                if (docs.length === 0) return this.insert(update.$set, {}, cb);
                if (update.$set._id) delete update.$set._id;
                for (const doc of docs) {
                    let doc2 = _.assign(doc, update.$set);
                    cmd.push(q.ninvoke(this.dataDb, 'put', doc._id, jsonfn.stringify(doc2)));
                    this.idx[_.findKey(this.idx, id => id._id === doc._id)] = this.getIndex(doc2);
                    cmd.push(q.ninvoke(this.indexDb, 'put', doc._id, jsonfn.stringify(this.getIndex(doc2))));
                }
                Promise.all(cmd)
                    .then(docs => {
                        cb(null, {value: docs, ok: 1})
                    })
                    .catch(err => cb(err))
            });
        })
    }

    update(query, update, opts = {}, cb) {
        if (!this.loaded) return this.queue.push(['update', arguments]);
        this.findAndModify(query, {}, update, null, cb);
    }

    remove(query, opts, cb) {
        if (!this.loaded) return this.queue.push(['remove', arguments]);
        normalize(query);
        let keys = processFind(this.idx, query, opts).map(doc => doc._id);

        _.remove(this.idx, i => keys.includes(i._id));

        if (keys.length > 0) {
            const cmd = [];
            for (const key of keys) {
                cmd.push(q.ninvoke(this.dataDb, 'del', key));
                cmd.push(q.ninvoke(this.indexDb, 'del', key));
            }
            Promise.all(cmd)
                .then(() => {
                    cb(null, keys.length);
                })
                .catch(err => cb(err))
        } else {
            cb(null, 0);
        }
    }

    onClose(force) {
        super.onClose(force);
    }

}

function processFind(items = [], query, opts) {
    let filtered = sift(query, items);
    if (opts && opts.sort) filtered.sort(compileSort(opts.sort))
    if (opts && opts.skip) filtered = _.drop(filtered, opts.skip)
    if (opts && opts.limit) filtered = _.take(filtered, opts.limit)
    return filtered;
}

const normalize = function (obj) {
    _.each(obj, function (v, k) {
        if (v instanceof ObjectId) {
            obj[k] = v.toString();
        } else if (_.isObject(v)) {
            if (v.isMongooseArray) {
                obj[k] = obj[k].toObject();
            } else {
                normalize(v);
            }
        }
    });
    return obj;
};

/*!
 * Module exports.
 */

module.exports = TingoCollection;
