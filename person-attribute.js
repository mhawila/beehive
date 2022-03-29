'use strict';
const utils = require('./utils');
const strValue = utils.stringValue;
const uuid = utils.uuid;
const moveAllTableRecords = utils.moveAllTableRecords;
const config = require('./config');

const BATCH_SIZE = config.batchSize || 200;
let beehive = global.beehive;
beehive.attributeTypesIdsWithLocationValue = [];

function preparePersonAttributeTypeInsert(rows, nextId) {
    let insert = 'INSERT INTO person_attribute_type(person_attribute_type_id, ' +
        'name, description, format, foreign_key, searchable, creator, ' +
        'date_created, changed_by, date_changed, retired, retired_by, ' +
        'date_retired, retire_reason, edit_privilege, uuid, sort_weight) ' +
        'VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['format'])}, ` +
            `${row['foreign_key']}, ${row['searchable']}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ${row['retired']}, ` +
            `${retiredBy}, ${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${strValue(row['edit_privilege'])}, ` +
            `${uuid(row['uuid'])}, ${row['sort_weight']})`;

        //Update the map
        beehive.personAttributeTypeMap.set(row['person_attribute_type_id'], nextId);
        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function preparePersonAttributeInsert(rows, nextId) {
    let insert = 'INSERT INTO person_attribute(person_attribute_id, person_id, ' +
        'value, person_attribute_type_id, creator, date_created, changed_by, ' +
        'date_changed, voided, voided_by, date_voided, void_reason, uuid) ' +
        'VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        let attributeValue = row['value'];

        if(beehive.attributeTypesIdsWithLocationValue.includes(row['person_attribute_type_id'])) {
            attributeValue = beehive.locationMap.get(parseInt(row['value'])).toString();
        }

        toBeinserted += `(${nextId}, ${beehive.personMap.get(row['person_id'])}, ` +
            `${strValue(attributeValue)}, ` +
            `${beehive.personAttributeTypeMap.get(row['person_attribute_type_id'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${uuid(row['uuid'])})`

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function consolidatePersonAttributeTypes(srcConn, destConn) {
    let query = 'SELECT * FROM person_attribute_type';
    let toAdd = [], stmt;
    try {
        let [sAttributeTypes] = await srcConn.query(query);
        let [dAttributeTypes] = await destConn.query(query);
        sAttributeTypes.forEach(sAttributeType => {
            let match = dAttributeTypes.find(dAttributeType => {
                return (sAttributeType['name'] === dAttributeType['name'] ||
                            sAttributeType['uuid'] === dAttributeType['uuid']);
            });
            if (match !== undefined) {
                beehive.personAttributeTypeMap.set(sAttributeType['person_attribute_type_id'],
                    match['person_attribute_type_id']);
            } else {
                toAdd.push(sAttributeType);
            }
        });
        if (toAdd.length > 0) {
            let nextId = await utils.getNextAutoIncrementId(destConn, 'person_attribute_type');

            [stmt] = preparePersonAttributeTypeInsert(toAdd, nextId);
            await destConn.query(stmt);
        }
    }
    catch(ex) {
        utils.logError(`An error occured when consolidating attribute types`);
        if(stmt) {
            utils.logError('Statement During error:');
            utils.logError(stmt);
        }
        throw ex;
    }
}

async function findPersonAttributeTypesWithLocationValue(srcConn) {
    let query = `SELECT person_attribute_type_id from ${config.source.openmrsDb}.person_attribute_type ` +
                `where format='org.openmrs.Location'`;
    try {
        let [rows] = await srcConn.query(query);
        rows.forEach(row => {
            beehive.attributeTypesIdsWithLocationValue.push(row['person_attribute_type_id']);
        });
    }
    catch(ex) {
        utils.logError(`An error occured when fetching person attribute types whose value is of format org.openmrs.Location`)
        utils.logError(`Query During error: `, query);
        throw ex;
    }
}

async function movePersonAttributes(srcConn, destConn) {
    let excluded = '(' + global.excludedPersonIds.join(',') + ')';
    let condition = `person_id NOT IN ${excluded}`;
    return await moveAllTableRecords(srcConn, destConn, 'person_attribute',
        'person_attribute_id', preparePersonAttributeInsert, condition);
}

async function main(srcConn, destConn) {
    utils.logInfo('Copying person attributes to destination...');
    await consolidatePersonAttributeTypes(srcConn, destConn);
    findPersonAttributeTypesWithLocationValue(srcConn);
    await movePersonAttributes(srcConn, destConn);
    utils.logOk(`Ok... Person attributes moved`)
}

module.exports = main;