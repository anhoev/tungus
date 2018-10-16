console.log('Running mongoose version %s');
require('../index');
const mongoose = require('mongoose');

const Schema = mongoose.Schema;
const _ = require('lodash');
const autopopulate = require('mongoose-autopopulate');

mongoose.connect('tingodb://test', {useMongoClient: false}, function (err) {
    // if we failed to connect, abort
    if (err) throw err;

    // we connected ok
})


const consoleSchema = Schema({
    name: {type: String, index: true},
    vid: {type: Number, index: true},
    manufacturer: String,
    released: Date
});
const Console = mongoose.model('Console', consoleSchema);

/**
 * Game schema
 */
const gameSchema = Schema({
    name: String,
    developer: String,
    released: Date,
    console1: {type: Schema.Types.ObjectId, ref: 'Console', autopopulate: true}
});

gameSchema.plugin(autopopulate);

const Game = mongoose.model('Game', gameSchema);

/**
 * Connect to the console database on localhost with
 * the default port (27017)
 */

mongoose.Promise = global.Promise;

/**
 * Population
 */

example();

async function example() {
    try {
        let consoles = await Console.find({});
        console.log(consoles);
        //await Console.remove({});
        const name = 'test';
        await Console.create({name, manufacturer: name + '2', vid: 1, released: new Date()});
        await Console.create({name, manufacturer: name + '3', vid: 1, released: new Date()});
        Console.collection.rebuildIndex();
        console.time('findOne');
        const console1 = await Console.findOne({});
        console.timeEnd('findOne');
        console1.vid = 20;
        await Console.collection.modifyById(console1);
        //console1.save();

        /*await Console.findOne({});
        for (let i = 0; i < 100; i++) {
            const name = 'test';
            await Console.create({name, manufacturer: name + '2', vid: i});
        }*/

        //await Console.count();

        /*console.time('find');
        const consoles = await Console.find({}).lean()
        console.timeEnd('find');*/

        /*const console1 = await Console.findOne({name: {$eq: 'test'}}).lean();

        await Game.remove({});
        await Game.create({
            name: 'Legend of Zelda: Ocarina of Time',
            developer: 'Nintendo',
            released: new Date('November 21, 1998'),
            console1,
        });

        //const count = await Game.count({});

        const game = await Game.findOne({});
        game.name = 'HHH';
        await game.save();

        const game1 = await Game.findOne({console: {$exists: false}});
        console.log(game1.name);*/
        const a = 5;
        await mongoose.disconnect();
    } catch (e) {
        console.warn(e);
    }
}