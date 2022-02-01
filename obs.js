let utils = require('./utils');
const { Worker } = require('worker_threads');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;
let obsWithTheirGroupNotUpdated = {};
let obsWithPreviousNotUpdated = {};

function prepareObsInsert(rows, nextId) {
  let insert = 'INSERT IGNORE INTO obs(obs_id, person_id, concept_id, encounter_id, '
        + 'order_id, obs_datetime, location_id, obs_group_id, accession_number, '
        + 'value_group_id, value_boolean, value_coded, value_coded_name_id, '
        + 'value_drug, value_datetime, value_numeric, value_modifier, '
        + 'value_text, value_complex, comments, previous_version, creator, '
        + 'date_created, voided, voided_by, '
        + 'date_voided, void_reason, uuid, form_namespace_and_path) VALUES ';

    let toBeinserted = '';
    for(let i = 0; i < rows.length; i++) {
        let row = rows[i];
        if(!global.excludedObsIds.includes(row['obs_id'])) {
            if(toBeinserted.length > 1) {
                toBeinserted += ',';
            }
    
            let voidedBy = row['voided_by'] === null ? null : beehive.userMap[row['voided_by']];
            let obsGroupsId = row['obs_group_id'] === null ? null : beehive.obsMap[row['obs_group_id']];
            let previous = row['previous_version']=== null ? null : beehive.obsMap[row['previous_version']];
            let encounterId = row['encounter_id'] === null ? null : beehive.encounterMap[row['encounter_id']];
            let locationId = row['location_id'] === null ? null : beehive.locationMap[row['location_id']];
            beehive.obsMap[row['obs_id']] = nextId;
            
            if(obsGroupsId === undefined) {
                // The new value of obs_group_id is not yet known because the associated obs is not yet copied.
                obsGroupsId = null;
                if(row['obs_group_id'] !== null) {
                    obsWithTheirGroupNotUpdated[nextId] = row['obs_group_id'];
                }
            }
        
            if(previous === undefined) {
                previous = null;
                if(row['previous_version'] !== null) {
                    obsWithPreviousNotUpdated[nextId] = row['previous_version'];
                }
            }
    
            toBeinserted += `(${nextId}, ${beehive.personMap[row['person_id']]}, `
                + `${row['concept_id']},  ${encounterId}, `
                + `${row['order_id']}, ${strValue(utils.formatDate(row['obs_datetime']))}, `
                + `${locationId}, ${obsGroupsId}, `
                + `${strValue(row['accession_number'])}, ${row['value_group_id']}, `
                + `${row['value_boolean']}, ${row['value_coded']}, `
                + `${row['value_coded_name_id']}, ${row['value_drug']}, `
                + `${strValue(utils.formatDate(row['value_datetime']))}, `
                + `${row['value_numeric']}, ${strValue(row['value_modifier'])}, `
                + `${strValue(row['value_text'])}, ${strValue(row['value_complex'])}, `
                + `${strValue(row['comments'])}, ${previous}, `
                + `${beehive.userMap[row['creator']]}, `
                + `${strValue(utils.formatDate(row['date_created']))}, `
                + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
                + `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}, `
                + `${strValue(row['form_namespace_and_path'])})`;
        
            nextId++;
        }
    }

    if(toBeinserted === '') {
        return [null, nextId];
    }

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function moveObs(srcConn, destConn) {
    let condition = null;
    if(global.excludedObsIds.length > 0) {
        condition = `obs_id NOT IN (${global.excludedObsIds.join(',')})`;
    }
    return await moveAllTableRecords(srcConn, destConn, 'obs', 'date_created',
                  prepareObsInsert, condition);
}

async function updateObsPreviousOrGroupId(connection, idMap, field) {
    let mapEntries = Object.entries(idMap);
    if(mapEntries.length > 0) {
        let update = `INSERT INTO obs(obs_id, ${field}) VALUES `;
        let lastPart = ` ON DUPLICATE KEY UPDATE ${field} = VALUES(${field})`;

        let values = '';
        for(const [obsId, srcIdValue] of mapEntries) {
            if(values.length > 1) {
                values += ',';
            }
            values += `(${obsId}, ${beehive.obsMap[srcIdValue]})`;
        }

        let query = update + values + lastPart;
        utils.logDebug(`${field} update query:`, query);
        await connection.query(query);
    }
    return mapEntries.length;
}

module.exports = async function(srcConn, destConn) {
    utils.logInfo('Moving obs...');
    let srcObsCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'obs');
    let initialDestCount = await utils.getCount(destConn, 'obs');
    let expectedFinalCount = initialDestCount + srcObsCount;

    // Create shared Buffers ID maps required for copying obs.
    const USER_MAP_SIZE = Uint32Array.BYTES_PER_ELEMENT * beehive.userMap.length;
    const USER_MAP_BUFFER = new SharedArrayBuffer(USER_MAP_SIZE);
    const SHARED_USER_MAP = new Uint32Array(USER_MAP_BUFFER);
    beehive.userMap.forEach((val, key) => {
        Atomics.store(SHARED_USER_MAP, key, val);
    });

    const PERSON_MAP_SIZE = Uint32Array.BYTES_PER_ELEMENT * beehive.personMap.length;
    const PERSON_MAP_BUFFER = new SharedArrayBuffer(PERSON_MAP_SIZE);
    const SHARED_PERSON_MAP = new Uint32Array(PERSON_MAP_BUFFER);
    beehive.userMap.forEach((val, key) => {
        Atomics.store(SHARED_PERSON_MAP, key, val);
    });

    const LOCATION_MAP_SIZE = Uint32Array.BYTES_PER_ELEMENT * beehive.locationMap.length;
    const LOCATION_MAP_BUFFER = new SharedArrayBuffer(LOCATION_MAP_SIZE);
    const SHARED_LOCATION_MAP = new Uint32Array(LOCATION_MAP_BUFFER);
    beehive.userMap.forEach((val, key) => {
        Atomics.store(SHARED_LOCATION_MAP, key, val);
    });

    const ENCOUNTER_MAP_SIZE = Uint32Array.BYTES_PER_ELEMENT * beehive.encounterMap.length;
    const ENCOUNTER_MAP_BUFFER = new SharedArrayBuffer(ENCOUNTER_MAP_SIZE);
    const SHARED_ENCOUNTER_MAP = new Uint32Array(ENCOUNTER_MAP_BUFFER);
    beehive.userMap.forEach((val, key) => {
        Atomics.store(SHARED_ENCOUNTER_MAP, key, val);
    });

    let nextAutoIncrId = await utils.getNextAutoIncrementId(destConn, 'obs');
    const OBS_MAP_SIZE = Uint32Array.BYTES_PER_ELEMENT * utils.subtractDecimalNumbers(nextAutoIncrId, 1);
    const OBS_MAP_BUFFER = new SharedArrayBuffer(OBS_MAP_SIZE);
    const SHARED_OBS_MAP = new Uint32Array(OBS_MAP_BUFFER);
    beehive.obsMap.forEach((val, key) => {
        Atomics.store(SHARED_OBS_MAP, key, val);
    });
    // Use workers for large number of obs. (Use 8 for now)
    const NU_WORKERS = 8;
    const REMAINDER_RECORDS = srcObsCount % NU_WORKERS;
    const OBS_CHUNK_SIZE = Math.floor(srcObsCount/NU_WORKERS);
    const SHARED_DATA = {
        workerData: {
            obsMap: SHARED_OBS_MAP,
            userMap: SHARED_USER_MAP,
            encounterMap: SHARED_ENCOUNTER_MAP,
            personMap: SHARED_PERSON_MAP,
            locationMap: SHARED_LOCATION_MAP,
            start: 0,
            countToMove: (REMAINDER_RECORDS > 0 ? 
                utils.addDecimalNumbers(OBS_CHUNK_SIZE, REMAINDER_RECORDS) : OBS_CHUNK_SIZE),
            nextId: nextAutoIncrId,
            threadId: 1
        }
    };
    const workers = new Array(NU_WORKERS);
    workers[0] = new Worker('./obs-worker.js', SHARED_DATA);

    let start = REMAINDER_RECORDS;
    let copiedObs = 0;
    for(let i=1; i < NU_WORKERS; i++) {
        start = utils.addDecimalNumbers(start, OBS_CHUNK_SIZE);
        SHARED_DATA['workerData']['start'] = start;
        SHARED_DATA['workerData']['countToMove'] = OBS_CHUNK_SIZE;
        SHARED_DATA['workerData']['nextId'] = utils.addDecimalNumbers(SHARED_DATA['workerData']['nextId'], OBS_CHUNK_SIZE);
        SHARED_DATA['workerData']['threadId'] = i + 1;
        workers[i] = new Worker('./obs-worker.js', SHARED_DATA);
    }

    for(let i=0; i < NU_WORKERS; i++) {
        workers[i].on('error', err => { throw err; });
        workers[i].on('exit', () => {
            delete workers[i];
            utils.logDebug(`Worker thread ${i+1} exiting...`);
            if(workers.length === 0) {
                utils.logOk(`Ok... ${copiedObs} obs copied to destination.`);
            }
        });
        workers[i].on('message', numberCopied => {
            copiedObs = utils.addDecimalNumbers(copiedObs, numberCopied);
        });
    }
    // let moved = await moveObs(srcConn, destConn);
    // let finalDestCount = await utils.getCount(destConn, 'obs');

    // if (finalDestCount === expectedFinalCount) {
    //     // Update obs_group_id & previous_version for records not yet updated
    //     await updateObsPreviousOrGroupId(destConn, obsWithTheirGroupNotUpdated, 'obs_group_id');
    //     await updateObsPreviousOrGroupId(destConn, obsWithPreviousNotUpdated, 'previous_version');
    //     utils.logOk(`Ok... ${moved} obs moved.`);
    // } else {
    //     let error = `Problem moving obs: the actual final count ` +
    //         `(${expectedFinalCount}) is not equal to the expected value ` +
    //         `(${finalDestCount})`;
    //     throw new Error(error);
    // }
}
