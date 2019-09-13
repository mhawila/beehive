'use strict'
const connection = require('../connection').connection
const config = require('../config')

async function getSourcesLastStep(callback) {
    let query = 'SELECT t.source, b.atomic_step, b.passed, t.time_finished FROM ' +
            '(SELECT max(id) id, source, max(time_finished) time_finished from beehive_merge_progress ' +
            'group by source) t inner join beehive_merge_progress b using(id)';

    let sources = [];
    try {
        let destConn = await connection(config.destination);
        [sources] = await destConn.query(query);
    } catch(err) {
        console.error(err);
    } finally {
        if(typeof callback === 'function') {
            callback(sources)
        } else {
            return sources;
        }
    }
}

module.exports = {
    getSourcesLastStep: getSourcesLastStep,
}
