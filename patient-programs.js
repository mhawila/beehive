const utils = require('./utils');
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const moveAllTableRecords = utils.moveAllTableRecords;

let beehive = global.beehive;
beehive.programMap = new Map();
beehive.programWorkflowMap = new Map();
beehive.patientProgramMap = new Map();

function prepareProgramInsert(rows, nextId) {
    let insert = 'INSERT INTO program(program_id, concept_id, creator, ' +
            'date_created, changed_by, date_changed, retired, name, ' +
            'description, uuid, outcomes_concept_id) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        beehive.programMap.set(row['program_id'], nextId);

        toBeinserted += `(${nextId}, ${row['concept_id']}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))},` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))},` +
            `${row['retired']}, ${strValue(row['name'])},` +
            `${strValue(row['description'])}, ${utils.uuid(row['uuid'])},` +
            `${row['outcomes_concept_id']})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareProgramWorkflowInsert(rows, nextId) {
    let insert = 'INSERT INTO program_workflow(program_workflow_id, ' +
            'program_id, concept_id, creator, date_created, retired, ' +
            'changed_by, date_changed, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.programWorkflowMap.set(row['program_workflow_id'], nextId);

        toBeinserted += `(${nextId}, ${beehive.programMap.get(row['program_id'])}, ` +
            `${row['concept_id']}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['retired']}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareProgramWorkflowStateInsert(rows, nextId) {
    let insert = 'INSERT INTO program_workflow_state(program_workflow_state_id, ' +
        'program_workflow_id, ' +
        'concept_id, initial, terminal, creator, date_created, retired, ' +
        'changed_by, date_changed, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.programWorkflowStateMap.set(row['program_workflow_state_id'], nextId);

        toBeinserted += `(${nextId}, ` +
            `${beehive.programWorkflowMap.get(row['program_workflow_id'])}, ` +
            `${row['concept_id']},  ${row['initial']}, ${row['terminal']}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${row['retired']}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${utils.uuid(row['uuid'])})`

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function preparePatientProgramInsert(rows, nextId) {
    let insert = 'INSERT INTO patient_program(patient_program_id, patient_id, ' +
        'program_id, ' +
        'date_enrolled, date_completed, creator, date_created, changed_by, ' +
        'date_changed, voided, voided_by, date_voided, void_reason, uuid, ' +
        'location_id, outcome_concept_id) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.patientProgramMap.set(row['patient_program_id'], nextId);

        toBeinserted += `(${nextId}, ${beehive.personMap.get(row['patient_id'])}, ` +
            `${beehive.programMap.get(row['program_id'])}, ` +
            `${strValue(utils.formatDate(row['date_enrolled']))}, ` +
            `${strValue(utils.formatDate(row['date_completed']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}` +
            `${beehive.locationMap.get(row['location_id'])}, ` +
            `${row['outcome_concept_id']})`

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function preparePatientStateInsert(rows) {
    let insert = 'INSERT INTO patient_state(patient_program_id, state, ' +
            'start_date, end_date, creator, date_created, changed_by, ' +
            'date_changed, voided, voided_by, date_voided, void_reason, ' +
            'uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${beehive.patientProgramMap.get(row['patient_program_id'])}, ` +
            `${beehive.programWorkflowStateMap.get(row['state'])}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ` +
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
    
}

module.exports = main;
