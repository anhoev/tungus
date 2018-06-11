const MongooseCollection = require('mongoose/lib/collection');
const _ = require('lodash');
const ObjectId = require('bson').ObjectId;


const levelup = require('levelup');
const leveldown = require('leveldown');
const {compileSort, compileDocumentSelector} = require('minimongo/lib/selector');
const utils = require('minimongo/lib/utils');
const document = require('linvodb3/lib/document')

class TingoCollection extends MongooseCollection {
    constructor() {
        super(...arguments);
        if (this.conn.uri) this.init();
    }

    init() {
        this.dataDb = levelup(leveldown(`${this.conn.uri.substr(10)}/${this.name}`));
        this.indexDb = levelup(leveldown(`${this.conn.uri.substr(10)}/${this.name}_index`));
        this.idx = [];
        this.indexes = [];
        this.indexDb.createReadStream()
            .on('data', data => {
                this.idx.push(document.deserialize(data.value));
            })
            .on('end', function () {
                console.log('Stream ended')
            })
    }

    onOpen() {
        if (!this.dataDb) this.init();
        super.onOpen();
    }

    getIndex(doc) {
        return _.pick(doc, this.indexes);
    }

    insert(doc, opt, cb) {
        normalize(doc);
        this.idx.push(doc);
        this.indexDb.put(doc._id, document.serialize(this.getIndex(doc))).then();
        this.dataDb.put(doc._id, document.serialize(doc), cb);
    }

    drop(cb) {
        cb();
    }

    ensureIndex(obj, options, cb) {
        const fieldName = _.map(obj, (v, k) => k)[0];
        this.indexes.push(fieldName);
    }

    createIndex(obj, options) {
        const fieldName = _.map(obj, (v, k) => k)[0];
        this.indexes.push(fieldName);
    }

    findOne(query, opts, _cb) {
        if (opts) delete opts.fields;
        normalize(query);
        let [key] = utils.processFind(this.idx, opts, {}).map(doc => doc._id);
        if (key) this.dataDb.get(key, (err, doc) => {
            _cb(err, document.deserialize(doc));
        });
    }

    find(query, opts, _cb) {
        if (opts) delete opts.fields;
        normalize(query);
        let keys = utils.processFind(this.idx, query, opts).map(doc => doc._id);
        //let docs = [];
        const cb = function (err, docs) {
            _cb(err, {
                toArray: cb2 => {
                    cb2(null, docs);
                }
            })
        }

        Promise.all(keys.map(_id => this.dataDb.get(_id)))
            .then(docs => {
                docs = docs.map(doc => document.deserialize(doc));
                cb(null, docs)
            })
            .catch(err => cb(err))


    }

    findAndModify(query, sort, update, opts = {}, cb) {
        normalize(update);
        if (update.$set._id) delete update.$set._id;
        if (update.$setOnInsert) delete update.$setOnInsert;

        this.find(query, opts, (err, _docs) => {
            _docs.toArray((err, docs) => {
                const batch = this.dataDb.batch();
                for (const doc of docs) {
                    let doc2 = _.assign(doc, update.$set);
                    batch.put(doc._id, document.serialize(doc2));
                    this.idx[this.idx.indexOf(doc._id)] = this.getIndex(doc2);
                }
                batch.write(() => {
                    cb(null, {value: docs, ok: 1})
                });
            });
        })
    }

    update(query, update, opts = {}, cb) {
        this.findAndModify(query, {}, update, null, cb);
    }

    remove(query, opts, cb) {
        normalize(query);
        let keys = utils.processFind(this.idx, query, {}).map(doc => doc._id);

        _.remove(this.idx, i => keys.includes(i._id));

        if (keys.length > 0) {
            const batchIndex = this.indexDb.batch();
            for (const key of keys) {
                if (key) batchIndex.del(key);
            }
            batchIndex.write(() => null);

            const batch = this.dataDb.batch();
            for (const key of keys) {
                if (key) batch.del(key);
            }
            batch.write(() => {
                cb(null, keys.length);
            });
        } else {
            cb(null, 0);
        }
    }

    onClose(force) {
        super.onClose(force);
    }

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
