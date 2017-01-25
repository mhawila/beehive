'use strict';
const connection = require('./connection').connection;
const prepare = require('./preparation').prepare;
const movePersonsUsersAndAssociatedTables = require('./person-users');
const locationsMover = require('./location');
const patientsMover = require('./patient');
const utils = require('./utils');
const logTime = utils.logTime;
const config = require('./config');


async function orchestration() {
    let initialErrors = [];
    if (config.source.location === undefined) {
        initialErrors.push('Error: source.location not specified in config.json file');
    }

    if (config.generateNewUuids === undefined) {
        let msg = 'Error: generateNewUuids option must be explicitly set to true/false ' +
            'in config.json file';
        initialErrors.push(msg);
    }

    if (initialErrors.length > 0) {
        initialErrors.forEach(error => {
            utils.logError(error);
        });
        utils.logInfo('Aborting...');
        process.exit(1);
    }

    let srcConn = null;
    let destConn = null;
    try {
        srcConn = await connection(config.source);
        destConn = await connection(config.destination);

        utils.logInfo(logTime(), ': Preparing destination database...');
        await prepare(destConn, config.source.location);

        utils.logInfo(logTime(), ': Starting data migration ...');
        destConn.query('START TRANSACTION');
        await movePersonsUsersAndAssociatedTables(srcConn, destConn);

        utils.logInfo('Consolidating locations...');
        let movedLocations = await locationsMover(srcConn, destConn);
        utils.logOk(`Ok...${movedLocations} locations moved.`);

        //patients & identifiers
        await patientsMover(srcConn, destConn);
        destConn.query('ROLLBACK');
    } catch (ex) {
        utils.logError(ex);
        utils.logInfo('Aborting...No data has been moved');
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
    }
}

module.exports = orchestration; // In case one needs to require it.

// Run
orchestration();
