const utils = require('./utils');
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;
beehive.gaacMap = new Map();
beehive.gaacAffinityTypeMap = new Map();
beehive.gaacReasonLeavingTypeMap = new Map();

function _prepareGaacTypeInsertTemplate(rows, nextId, type, typeMap) {
  let insert = `INSERT INTO gaac_${type}_type(gaac_${type}_type_id, name, ` +
        'description, creator, date_created, retired, retired_by,' +
        'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);

      typeMap.set(row[`gaac_${type}_type_id`], nextId);

      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, `
          + `${beehive.userMap.get(row['creator'])}, `
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
    let insert = 'INSERT INTO gaac(gaac_id, name, description, ' +
            'gaac_identifier, start_date, end_date, focal_patient_id, ' +
            'affinity_type, location_id, crumbled, reason_crumbled, ' +
            'date_crumbled, creator, date_created, changed_by, date_changed, ' +
            'voided, voided_by, date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null :
                                beehive.userMap.get(row['voided_by']));
        let changedBy = (row['changed_by'] === null ? null :
                            beehive.userMap.get(row['changed_by']));
        let locationId = (row['location_id'] === null ? null :
                            beehive.locationMap.get(row['location_id']));
        let affinityType = (row['affinity_type'] === null ? null :
                            beehive.gaacAffinityTypeMap.get(row['affinity_type']));
        let focalPatientId = (row['focal_patient_id'] === null ? null :
                            beehive.personMap.get(row['focal_patient_id']));

        beehive.gaacMap.set(row['gaac_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['gaac_identifier'])}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ` +
            `${focalPatientId}, ${affinityType}, ${locationId}, ` +
            `${row['crumbled']}, ${strValue(row['reason_crumbled'])}, ` +
            `${strValue(utils.formatDate(row['date_crumbled']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
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
    let insert = 'INSERT INTO gaac_member(gaac_id, member_id, start_date, ' +
            'end_date, reason_leaving_type, description, leaving, restart, ' +
            'restart_date, creator, date_created, changed_by, date_changed, ' +
            'voided, voided_by, date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null :
                                beehive.userMap.get(row['voided_by']));
        let changedBy = (row['changed_by'] === null ? null :
                            beehive.userMap.get(row['changed_by']));
        let reasonLeavingType = (row['reason_leaving_type'] === null ? null :
                beehive.gaacReasonLeavingTypeMap.get(row['reason_leaving_type']));


        toBeinserted += `(${beehive.gaacMap.get(row['gaac_id'])}, ` +
            `${beehive.personMap.get(row['member_id'])}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ${reasonLeavingType}, ` +
            `${strValue(row['description'])}, ${row['leaving']}, ${row['restart']}, ` +
            `${strValue(utils.formatDate(row['restart_date']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`;
    });

    let query = insert + toBeinserted;
    return [query, -1];
}

async function main(srcConn, destConn) {
    // TODO
}

module.exports = main;
