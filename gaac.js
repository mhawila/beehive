const utils = require('./utils');
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const moveAllTableRecords = utils.moveAllTableRecords;
const consolidateRecords = utils.consolidateRecords;

let beehive = global.beehive;
beehive.gaacMap = new Map();
beehive.gaacAffinityTypeMap = new Map();
beehive.gaacReasonLeavingTypeMap = new Map();
beehive.excludedGaacIds = [];

function _prepareGaacTypeInsertTemplate(rows, nextId, type, typeMap) {
  let insert = `INSERT IGNORE INTO gaac_${type}_type(gaac_${type}_type_id, name, ` +
        'description, creator, date_created, retired, retired_by,' +
        'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : beehive.userMap[row['retired_by']];

      typeMap[row[`gaac_${type}_type_id`]] =  nextId;

      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, `
          + `${beehive.userMap[row['creator']]}, `
          + `${strValue(utils.formatDate(row['date_created']))}, `
          + `${row['retired']}, ${retiredBy}, `
          + `${strValue(utils.formatDate(row['date_retired']))}, `
          + `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

      nextId++;
  });

  let query = insert + toBeinserted;
  return [query, nextId];
}

function prepareGaacAffinityTypeInsert(rows, nextId) {
    return _prepareGaacTypeInsertTemplate(rows, nextId, 'affinity',
                beehive.gaacAffinityTypeMap);
}

function prepareGaacReasonLeavingTypeInsert(rows, nextId) {
    return _prepareGaacTypeInsertTemplate(rows, nextId, 'reason_leaving',
                beehive.gaacReasonLeavingTypeMap);
}

function prepareGaacInsert(rows, nextId) {
    let insert = 'INSERT IGNORE INTO gaac(gaac_id, name, description, ' +
            'gaac_identifier, start_date, end_date, focal_patient_id, ' +
            'affinity_type, location_id, crumbled, reason_crumbled, ' +
            'date_crumbled, creator, date_created, changed_by, date_changed, ' +
            'voided, voided_by, date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null :beehive.userMap[row['voided_by']]);
        let changedBy = (row['changed_by'] === null ? null :beehive.userMap[row['changed_by']]);
        let locationId = (row['location_id'] === null ? null :beehive.locationMap[row['location_id']]);
        let affinityType = (row['affinity_type'] === null ? null : beehive.gaacAffinityTypeMap[row['affinity_type']]);
        let focalPatientId = (row['focal_patient_id'] === null ? null : beehive.personMap[row['focal_patient_id']]);

        beehive.gaacMap[row['gaac_id']] =  nextId;

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['gaac_identifier'])}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ` +
            `${focalPatientId}, ${affinityType}, ${locationId}, ` +
            `${row['crumbled']}, ${strValue(row['reason_crumbled'])}, ` +
            `${strValue(utils.formatDate(row['date_crumbled']))}, ` +
            `${beehive.userMap[row['creator']]}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareGaacMemberInsert(rows) {
    let insert = 'INSERT IGNORE INTO gaac_member(gaac_id, member_id, start_date, ' +
            'end_date, reason_leaving_type, description, leaving, restart, ' +
            'restart_date, creator, date_created, changed_by, date_changed, ' +
            'voided, voided_by, date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null : beehive.userMap[row['voided_by']]);
        let changedBy = (row['changed_by'] === null ? null :beehive.userMap[row['changed_by']]);
        let reasonLeavingType = (row['reason_leaving_type'] === null ? null : beehive.gaacReasonLeavingTypeMap[row['reason_leaving_type']]);


        toBeinserted += `(${beehive.gaacMap[row['gaac_id']]}, ` +
            `${beehive.personMap[row['member_id']]}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ${reasonLeavingType}, ` +
            `${strValue(row['description'])}, ${row['leaving']}, ${row['restart']}, ` +
            `${strValue(utils.formatDate(row['restart_date']))}, ` +
            `${beehive.userMap[row['creator']]}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`;
    });

    let query = insert + toBeinserted;
    return [query, -1];
}

async function consolidateGaacAffinityTypes(srcConn, destConn) {
    return await consolidateRecords(srcConn, destConn, 'gaac_affinity_type',
            'name', 'gaac_affinity_type_id', beehive.gaacAffinityTypeMap,
            prepareGaacAffinityTypeInsert);
}

async function consolidateGaacReasonLeavingTypes(srcConn, destConn) {
    return await consolidateRecords(srcConn, destConn, 'gaac_reason_leaving_type',
            'name', 'gaac_reason_leaving_type_id', beehive.gaacReasonLeavingTypeMap,
            prepareGaacReasonLeavingTypeInsert);
}

async function moveGaacs(srcConn, destConn) {
    let condition = null;
    if(beehive.excludedGaacIds.length > 0) {
        condition = `gaac_id NOT IN (${beehive.excludedGaacIds.join(',')})`;
    }
    return await moveAllTableRecords(srcConn, destConn, 'gaac', 'gaac_id',
                    prepareGaacInsert, condition);
}

async function moveGaacMembers(srcConn, destConn) {
    return await moveAllTableRecords(srcConn, destConn, 'gaac_member',
                    'gaac_member_id', prepareGaacMemberInsert);
}

async function main(srcConn, destConn) {
    utils.logInfo('Moving GAAC module tables');

    utils.logInfo('Checking if gaac module tables exists in source');
    let [r] = await srcConn.query(`SHOW TABLES LIKE 'gaac%'`);

    if(r.length === 0) {
        utils.logInfo('No gaac tables found');
        return;
    }
    
    // Map same uuid records in gaac tables between source and destination
    await utils.mapSameUuidsRecords(srcConn, 'gaac', 'gaac_id', beehive.gaacMap, beehive.excludedGaacIds);
    
    utils.logInfo('Consolidating GAAC Affinity types...');
    let moved = await consolidateGaacAffinityTypes(srcConn, destConn);
    utils.logOk(`Ok...${moved} records from gaac_affinity_type moved`);

    utils.logInfo('Consolidating GAAC Reason for leaving types...');
    moved = await consolidateGaacReasonLeavingTypes(srcConn, destConn);
    utils.logOk(`Ok...${moved} records from gaac_reason_leaving_type moved`);

    utils.logInfo('Moving Gaacs...');
    let iDestCount = await utils.getCount(destConn, 'gaac');
    moved = await moveGaacs(srcConn, destConn);

    let fDestCount = await utils.getCount(destConn, 'gaac');
    let expectedFinal = iDestCount + moved;

    if(fDestCount === expectedFinal) {
        utils.logOk(`OK... ${moved} gaac records moved.`);

        utils.logInfo('Moving gaac_member records...');
        iDestCount = await getCount(destConn, 'gaac_member');

        moved = await moveGaacMembers(srcConn, destConn);

        fDestCount = await getCount(destConn, 'gaac_member');
        expectedFinal = iDestCount + moved;
        if (fDestCount === expectedFinal) {
            utils.logOk(`OK... ${moved} gaac_member records moved.`);
        } else {
            let message = 'There is a problem in moving gaac_member records, ' +
                'the final expected ' +
                `count (${expectedFinal}) does not equal the actual final ` +
                `count (${fDestCount})`;
            throw new Error(message);
        }
    }
    else {
        let message = 'There is a problem in moving gaac records, ' +
            'the final expected ' +
            `count (${expectedFinal}) does not equal the actual final ` +
            `count (${fDestCount})`;
        throw new Error(message);
    }
}

module.exports = main;
