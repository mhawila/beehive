'use strict';
const connection = require('./connection').connection;
const preparation = require('./preparation');
const integrityChecks = require('./integrity-checks');
const prepare = preparation.prepare;
const insertSource = preparation.insertSource;
const movePersonsUsersAndAssociatedTables = require('./person-users');
const locationsCopier = require('./location');
const patientsCopier = require('./patient');
const programsCopier = require('./patient-programs');
const providersCopier = require('./provider');
const visitsCopier = require('./visit');
const encounterCopier = require('./encounter');
const personAttributesCopier = require('./person-attribute');
const obsCopier = require('./obs');
const gaacModuleTablesCopier = require('./gaac');
const utils = require('./utils');
const logTime = utils.logTime;
const config = require('./config');
global.openmrsDataModelVersion = 1;

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

        utils.logInfo('INFO: Detecting Openmrs Data Model Version');
        let getAllergyColumnQuery = (databaseName) => {
            return 'SELECT count(*) AS col_count from information_schema.COLUMNS ' + 
                `WHERE TABLE_SCHEMA = '${databaseName}' AND TABLE_NAME = 'patient' ` +
                `AND COLUMN_NAME = 'allergy_status'`;
        };
        
        let allergyQuery = getAllergyColumnQuery(config.source.openmrsDb);
        let [results] = await srcConn.query(allergyQuery);
        let srcColCount = results[0]['col_count'];

        allergyQuery = getAllergyColumnQuery(config.destination.openmrsDb);
        [results] = await destConn.query(allergyQuery);
        let destColCount = results[0]['col_count'];

        if(srcColCount !== destColCount) {
            utils.logInfo(`INFO: Copying between different openmrs data models, possible data losses`);
        } else if(srcColCount === 0 && destColCount === 0) {
            utils.logInfo(`INFO: Openmrs Data Model Version 1.x Detected`);
        } else if(srcColCount === 1 && destColCount === 1) {
            utils.logInfo(`INFO: Openmrs Data Model Version 2.x Detected`)
            global.openmrsDataModelVersion = 2;
        } else {
            utils.logInfo('INFO: Could not detect Openmrs Data Model Version, presuming 1.x');
        }

        if(dryRun) {
            utils.logInfo(logTime(), ': Partial preparation for DRY run');
            await preparation.prepareForDryRun(srcConn, destConn, config);
        } else {
            utils.logInfo(logTime(), ': Preparing destination database...');
            await prepare(srcConn,destConn, config);
        }

        utils.logInfo(logTime(), ': Checking for Orphaned Records');
        await integrityChecks(srcConn, config.source.openmrsDb);

        utils.logInfo(logTime(), ': Starting data migration ...');
        destConn.query('START TRANSACTION');
        await movePersonsUsersAndAssociatedTables(srcConn, destConn);

        utils.logInfo('Consolidating locations...');
        let movedLocations = await locationsCopier(srcConn, destConn);
        utils.logOk(`Ok...${movedLocations} locations moved.`);

        //patients & identifiers
        await patientsCopier(srcConn, destConn);

        //programs
        await programsCopier(srcConn, destConn);

        //providers & provider attributes
        await providersCopier(srcConn, destConn);

        //visits & visit types
        await visitsCopier(srcConn, destConn);

        //encounters, encounter_types, encounter_roles & encounter_providers
        await encounterCopier(srcConn, destConn);

        // person_attribute_type, person_attribute
        await personAttributesCopier(srcConn, destConn);
        
        //obs
        await obsCopier(srcConn, destConn);

        //gaac tables
        await gaacModuleTablesCopier(srcConn, destConn);

        if (!persist) {
            await insertSource(destConn, config.source.location);
        }

        if(dryRun) {
            destConn.query('ROLLBACK');
            utils.logOk(`Done...No database changes have been made!`)
        }
        else {
            destConn.query('COMMIT');
            utils.logOk(`Done...All Data from ${config.source.location} copied.`);
        }
    } catch (ex) {
        if(destConn) destConn.query('ROLLBACK');
        utils.logError(ex);
        utils.logInfo('Aborting...Rolled back, no data has been moved');
    } finally {
        if (srcConn) srcConn.end();
        if (destConn) destConn.end();
        let timeElapsed = (Date.now() - startTime);
        utils.logInfo(`Time elapsed: ${timeElapsed} ms`);
    }
}

module.exports = orchestration; // In case one needs to require it.

// Run
orchestration();
