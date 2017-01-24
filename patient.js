const utils = require('./utils');
const logLog = utils.logLog;
const logError = utils.logError;
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const moveAllTableRecords = utils.moveAllTableRecords;

const identifierTypeMap = new Map();

function preparePatientInsert(rows) {
  let insert = 'INSERT INTO patient(patient_id, tribe, creator, date_created, '
        + 'changed_by, date_changed, voided, voided_by, date_voided, '
        + 'void_reason) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let voidedBy = row['voided_by'] === null ? null : userMap.get(row['voided_by']);
      let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

      toBeinserted += `(${personMap.get(row['patient_id'])}, ${row['tribe']}, `
          + `${userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))},`
          + `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))},`
          + `${row['voided']}, ${voidedBy},`
          + `${strValue(utils.formatDate(row['date_voided']))},`
          + `${strValue(row['void_reason'])})`;
  });

  let query = insert + toBeinserted;
  return [query, -1];
}

function prepareIdentifierTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO patient_identifier_type(patient_identifier_type_id, '
        + 'name, description, format, check_digit, creator, date_created, '
        + 'required, format_description, validator, retired, retired_by, '
        + 'date_retired, retire_reason, uuid, location_behavior) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : userMap.get(row['retired_by']);

      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, ${strValue(row['format'])}, `
          + `${row['check_digit']}, ${userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, `
          + `${row['required']}, ${strValue(row['format_description'])}, `
          + `${strValue(row['validator'])}, ${row['retired']}, ${retiredBy}, `
          + `${strValue(utils.formatDate(row['date_retired']))}, `
          + `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])}, `
          + `${strValue(row['location_behavior'])})`;

      nextId++;
  });

  let query = insert + toBeinserted;
  return [query, nextId];
}

function preparePatientIdentifierInsert(rows, nextId) {
  let insert = 'INSERT INTO patient_identifier(patient_identifier_id, patient_id, '
        + 'identifier, identifier_type, preferred, location_id, creator, '
        + 'date_created, voided, voided_by, date_voided, void_reason, uuid, '
        + 'date_changed, changed_by) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let voidedBy = row['voided_by'] === null ? null : userMap.get(row['voided_by']);
      let changedBy = row['changed_by'] === null ? null : userMap.get(row['changed_by']);

      toBeinserted += `(${nextId}, ${personMap.get(row['patient_id'])}, `
          + `${strValue(row['identifier'])}, ${identifierTypeMap.get(row['identifier_type'])}, `
          + `${row['preferred']},  ${locationMap.get(row['location_id'])}, `
          + `${userMap.get(row['creator'])}, `
          + `${_handleString(utils.formatDate(row['date_created']))}, ${row['voided']}, `
          + `${voidedBy}, ${_handleString(utils.formatDate(row['date_voided']))}, `
          + `${_handleString(row['void_reason'])}, ${utils.uuid(row['uuid'])}, `
          + `${_handleString(utils.formatDate(row['date_changed']))}, ${changedBy})`

      nextId++;
  });

  let query = insert + toBeinserted;
  return [query, nextId];
}

async function movePatients(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'patient', 'patient_id',
                    preparePatientInsert);
}

async function movePatientIdentifiers(srcConn, destConn) {
  return await moveAllTableRecords(srcConn, destConn, 'patient_identifier',
                  'patient_identifier_id', preparePatientIdentifierInsert);
}

async function patientPhase(srcConn, destConn) {
    let iSrcPatientCount = await getCount(srcConn, 'patient');
    let iDestPatientCount = await getCount(destConn, 'patient');

    try{
      await destConn.query('START TRANSACTION');

      let moved = await movePatients(srcConn, destConn);

      let finalDestPatientCount = await getCount(destConn, 'patient');
      let expectedFinalCount = iDestPatientCount + moved;

      if(finalDestPatientCount === expectedFinalCount) {

        await destConn.query('COMMIT');
        logLog("\x1b[32m",`OK... ${logTime()}: ${moved} patients moved successlly`);
      }
      else {
        await destConn.query('ROLLBACK');

        let message = 'There is a problem in moving patients, the final expected '
              + `count (${expectedFinalCount}) does not equal the actual final `
              + `count (${finalDestPatientCount})`;
        logError(`Error: patient phase aborted`);
        logError(message);
      }
    }
    catch(ex) {
      await destConn.query('ROLLBACK');
      throw ex;
    }
}

module.exports.patientPhase = patientPhase;
