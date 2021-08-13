let utils = require('./utils');
let strValue = utils.stringValue;
let moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;

function prepareEncounterRoleInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO encounter_role(encounter_role_id, name, description, ' +
        'creator, date_created, changed_by, date_changed, retired, retired_by,' +
        'date_retired, retire_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.encounterRoleMap.set(row['encounter_role_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareEncounterProviderInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO encounter_provider(encounter_provider_id, ' +
        'encounter_id, provider_id, encounter_role_id, creator, date_created, ' +
        'changed_by, date_changed, voided, voided_by, date_voided, ' +
        'void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${nextId}, ${beehive.encounterMap.get(row['encounter_id'])}, ` +
            `${beehive.providerMap.get(row['provider_id'])}, ` +
            `${beehive.encounterRoleMap.get(row['encounter_role_id'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareEncounterTypeInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO encounter_type(encounter_type_id, name, ' +
        'description, creator, date_created, retired, retired_by, ' +
        'date_retired, retire_reason, uuid, view_privilege, edit_privilege, changed_by, date_changed) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.encounterTypeMap.set(row['encounter_type_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])}, ` + 
            `${strValue(row['view_privilege'])}, ${strValue(row['edit_privilege'])}, ` + 
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareEncounterInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO encounter(encounter_id, encounter_type, patient_id, ' +
        'location_id, form_id, visit_id, encounter_datetime, creator, date_created, ' +
        'changed_by, date_changed, voided, voided_by, date_voided, ' +
        'void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        let visitId = row['visit_id'] === null ? null : beehive.visitMap.get(row['visit_id']);
        beehive.encounterMap.set(row['encounter_id'], nextId);

        toBeinserted += `(${nextId}, ${beehive.encounterTypeMap.get(row['encounter_type'])}, ` +
            `${beehive.personMap.get(row['patient_id'])}, ` +
            `${beehive.locationMap.get(row['location_id'])}, ${row['form_id']}, ` +
            `${visitId}, ${strValue(utils.formatDate(row['encounter_datetime']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function consolidateEncounterTypes(srcConn, destConn) {
    let query = 'SELECT * FROM encounter_type';
    let [srcEncounterTypes] = await srcConn.query(query);
    let [destEncounterTypes] = await destConn.query(query);

    let missingInDest = [];
    srcEncounterTypes.forEach(srcEncounterType => {
        let match = destEncounterTypes.find(destEncounterType => {
            return srcEncounterType['name'] === destEncounterType['name'];
        });

        if (match !== undefined && match !== null) {
            beehive.encounterTypeMap.set(srcEncounterType['encounter_type_id'],
                match['encounter_type_id']);
        } else {
            missingInDest.push(srcEncounterType);
        }
    });

    if (missingInDest.length > 0) {
        let nextEncounterTypeId =
            await utils.getNextAutoIncrementId(destConn, 'encounter_type');

        let [sql] = prepareEncounterTypeInsert(missingInDest, nextEncounterTypeId);
        let [result] = await destConn.query(sql);
        return result.affectedRows;
    }
    return 0;
}

async function consolidateEncounterRoles(srcConn, destConn) {
    let query = 'SELECT * FROM encounter_role';
    let [srcEncounterRoles] = await srcConn.query(query);
    let [destEncounterRoles] = await destConn.query(query);

    let missingInDest = [];
    srcEncounterRoles.forEach(srcEncounterRole => {
        let match = destEncounterRoles.find(destEncounterRole => {
            return srcEncounterRole['name'] === destEncounterRole['name'];
        });

        if (match !== undefined && match !== null) {
            beehive.encounterRoleMap.set(srcEncounterRole['encounter_role_id'],
                match['encounter_role_id']);
        } else {
            missingInDest.push(srcEncounterRole);
        }
    });

    if (missingInDest.length > 0) {
        let nextEncounterRoleId =
            await utils.getNextAutoIncrementId(destConn, 'encounter_role');

        let [sql] = prepareEncounterRoleInsert(missingInDest, nextEncounterRoleId);
        let [result] = await destConn.query(sql);
        return result.affectedRows;
    }
    return 0;
}

async function moveEncounters(srcConn, destConn) {
    let condition = false;
    if(global.excludedEncounterIds.length > 0) {
        condition = `encounter_id NOT IN (${global.excludedEncounterIds.join(',')})`;
    } 
    return await moveAllTableRecords(srcConn, destConn, 'encounter',
        'encounter_id', prepareEncounterInsert, condition);
}

async function moveEncounterProviders(srcConn, destConn) {
    return await moveAllTableRecords(srcConn, destConn, 'encounter_provider',
        'encounter_provider_id', prepareEncounterProviderInsert);
}

async function main(srcConn, destConn) {
    utils.logInfo('Consolidating encounter types...');
    let movedTypes = await consolidateEncounterTypes(srcConn, destConn);
    utils.logOk(`Ok... ${movedTypes} encounter types moved.`);

    utils.logInfo('Consolidating encounter roles...');
    let movedEncRoles = await consolidateEncounterRoles(srcConn, destConn);
    utils.logOk(`Ok... ${movedEncRoles} encounter roles moved.`);

    utils.logInfo('Moving encounters...');
    let srcEncCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'encounter');
    let initialDestCount = await utils.getCount(destConn, 'encounter');
    let expectedFinalCount = initialDestCount + srcEncCount;

    let moved = await moveEncounters(srcConn, destConn);
    utils.logDebug(`Expected number of encounters to be copied is ${srcEncCount}`);
    utils.logDebug(`Actual number of copied encounters is ${moved}`);

    let finalDestCount = await utils.getCount(destConn, 'encounter');
    if (finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${moved} encounters moved.`);
    } else {
        let error = `Problem moving encounters: the actual final count ` +
            `(${finalDestCount}) is not equal to the expected value ` +
            `(${expectedFinalCount})`;
        throw new Error(error);
    }

    utils.logInfo('Moving encounter providers...');
    let srcCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'encounter_provider');
    initialDestCount = await utils.getCount(destConn, 'encounter_provider');
    expectedFinalCount = initialDestCount + srcCount;

    moved = await moveEncounterProviders(srcConn, destConn);

    finalDestCount = await utils.getCount(destConn, 'encounter_provider');
    if (finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${moved} encounter_provider records moved.`);
    } else {
        let error = `Problem moving encounter_providers: the actual final count ` +
            `(${expectedFinalCount}) is not equal to the expected value ` +
            `(${finalDestCount})`;
        throw new Error(error);
    }
}

module.exports = main;
