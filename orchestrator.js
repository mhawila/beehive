'use strict';
const connection = require('./connection').connection;
const preparation = require('./preparation');
const uuidChecks = require('./uuid-checks');
const integrityChecks = require('./integrity-checks');
const prepare = preparation.prepare;
const insertSource = preparation.insertSource;
const movePersonsUsersAndAssociatedTables = require('./person-users');
const locationsMover = require('./location');
const patientsMover = require('./patient');
const programsMover = require('./patient-programs');
const providersMover = require('./provider');
const visitsMover = require('./visit');
const encounterMover = require('./encounter');
const obsMover = require('./obs');
const gaacModuleTablesMover = require('./gaac');
const utils = require('./utils');
const logTime = utils.logTime;
const config = require('./config');


async function orchestration() {
    let persist = config.persist || false;
    let startTime = Date.now();
    let initialErrors = [];
    let dryRun = process.argv.some(arg => (arg === '--dry-run'));

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

        // Check for UUID collisions

        // if(!dryRun) {
        //
        // }

        await destConn.query('START TRANSACTION');
        utils.logInfo(logTime(), ': Preparing destination database...');
        await prepare(srcConn,destConn, config);

        utils.logInfo(logTime(), ': Checking for Orphaned Records');
        await integrityChecks(srcConn, config.source.openmrsDb);

        // utils.logInfo(logTime(), ': Ensuring uniqueness of UUIDs');
        // await uuidChecks(srcConn, destConn, dryRun, true);

        utils.logInfo(logTime(), ': Starting data migration ...');
        await movePersonsUsersAndAssociatedTables(srcConn, destConn);

        utils.logInfo('Consolidating locations...');
        let movedLocations = await locationsMover(srcConn, destConn);
        utils.logOk(`Ok...${movedLocations} locations moved.`);

        //patients & identifiers
        await patientsMover(srcConn, destConn);

        //programs
        await programsMover(srcConn, destConn);

        //providers & provider attributes
        await providersMover(srcConn, destConn);

        //visits & visit types
        await visitsMover(srcConn, destConn);

        //encounters, encounter_types, encounter_roles & encounter_providers
        await encounterMover(srcConn, destConn);

        //obs
        await obsMover(srcConn, destConn);

        //gaac tables
        await gaacModuleTablesMover(srcConn, destConn);

        if (!persist) {
            await insertSource(destConn, config.source.location);
        }

        if(dryRun) {
            await destConn.query('ROLLBACK');
            utils.logOk(`Done...No database changes have been made!`)
        }
        else {
            if(destConn) await destConn.query('COMMIT');
            utils.logOk(`Done...All Data from ${config.source.location} copied.`);
        }
    } catch (ex) {
        if(destConn) await destConn.query('ROLLBACK');
        utils.logError(ex);
        utils.logInfo('Aborting...Rolled back, no data has been moved');
    } finally {
        if (srcConn) await srcConn.end();
        if (destConn) await destConn.end();
        let timeElapsed = (Date.now() - startTime);
        utils.logInfo(`Time elapsed: ${timeElapsed} ms`);
    }
}

module.exports = orchestration; // In case one needs to require it.

// Run
orchestration();
