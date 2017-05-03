'use strict';
const config = require('./config');
const utils = require('./utils');
const getConnection = require('./connection').connection;
const uuidGenerator = require('uuid/v1');

/**
 * ensureUniqueUuids: It is a utility function that is used to ensure the uuid
 * for similar tables in source & destination database instances are unique
 * (To avoid duplicate entry errors because uuid is key)
 *
 * @param params: A key=>value pair parameter object. Allowed keys are:
 *  connection: Object Database connection instance
 *  table: String (The name of the table to be checked)
 *  primaryKey: String (The name of primary key of the table, Default {table}_id)
 *  primaryKeyType: String (Default is 'INT')
 *  uuidField: String|Optional (Default value is 'uuid')
 *  condition: String|Optional (A condition to be concatenated to WHERE clause.
 *              Mostly used for skipping records (meant not to be moved))
 *  action: function|Optional (Encapsulate logic of what is to be done for records with
 *           same uuid, (connection, tableName, uuidField, and results are passed as parameters))
 * callback: function| Optional (A function to be called once it is done)
 *
 * @return A map of initial=>final uuid for records with duplicate uuids.
 */
async function ensureUniqueUuids(params) {
    if(params === undefined || params === null) {
        throw new Error('function expects a parameter object');
    }

    if(params.table === null || params.table === undefined) {
        throw new Error('table name must be provided as params.table field ' +
                    ' in the parameter object');
    }

    let uuid = 'uuid';
    let primaryKey = params.table + '_id';
    let primaryKeyType = 'INT';
    let action = _assignNewUuids;

    if(params.uuidField !== undefined && params.uuidField !== null) {
        uuid = params.uuidField;
    }

    if(params.primaryKey !== undefined && params.primaryKey !== null) {
        primaryKey = params.primaryKey;
    }

    if(params.primaryKeyType !== undefined && params.primaryKeyType !== null) {
        primaryKeyType = params.primaryKeyType;
    }

    if(typeof params.action === 'function') {
        action = params.action;
    }

    const srcDb = config.source.openmrsDb;
    const destDb = config.destination.openmrsDb;
    let query = `SELECT t1.${primaryKey},t1.${uuid} ` +
        `FROM ${srcDb}.${params.table} AS t1 WHERE `;

    if(params.condition) {
        query += `${params.condition} AND `;
    }

    query += `EXISTS (SELECT 1 FROM ${destDb}.${params.table} AS t2 WHERE ` +
        `t1.${uuid} = t2.${uuid})`;

    utils.logDebug(`Query hunting for duplicate UUIDs in ${params.table}:\n ${query}`);
    let [result] = await params.connection.query(query);

    let changed = [];
    if(result.length > 0) {
        changed = await action(params.connection, params.table, primaryKey,
                                                primaryKeyType, uuid, result);
    }

    if(typeof params.callback === 'function') {
        callback();
    }
    return changed;
}

function _prepareUpdateSQL(table, primaryKey, primaryKeyType, uuidField, rows) {
    if(rows.length === 0) return;

    const srcDb = config.source.openmrsDb;
    let sql = `UPDATE  ${srcDb}.${table} SET ${uuidField} = (CASE `

    let values = '';
    let changedRecords = [];
    let primaryKeysForWhere = '(';
    rows.forEach(row => {
        let primaryKeyValue = row[primaryKey];
        let newUuidValue = uuidGenerator();
        if(primaryKeyType.toLowerCase() === 'text') {
            primaryKeyValue = utils.stringValue(primaryKeyValue);
        }
        values += `WHEN ${primaryKey} = ${primaryKeyValue} THEN ${utils.stringValue(newUuidValue)} `;

        if(primaryKeysForWhere.length > 1) {
            primaryKeysForWhere += ',';
        }
        primaryKeysForWhere += primaryKeyValue;

        let record = {};
        record[primaryKey] = primaryKeyValue;
        record['initialUuid'] = row[uuidField];
        record['newUuid'] = newUuidValue;
        changedRecords.push(record);
    });
    values += 'END) ';
    primaryKeysForWhere += ')';
    let where = `WHERE ${primaryKey} IN ${primaryKeysForWhere}`;

    let statement = sql + values + where;
    return [statement, changedRecords];
}

async function _assignNewUuids(connection, table, primaryKey, primaryKeyType,
    uuidField, rows) {
    let [statement, changes] =
            _prepareUpdateSQL(table, primaryKey, primaryKeyType, uuidField, rows);

    try {
        let [affectedRows] = await connection.query(statement);
        return changes;
    }
    catch(ex) {
        utils.logError('SQL statement during error:');
        utils.logError(statement);
        throw ex;
    }
}

function prettyPrintUuidChanges(changes) {
    // First column width will either be 10 or the key name length whichever is
    // greater. The initial UUID column and New UUID will both be 40 wide each.

    // Begin printing header
    const _printBorder = (columnWidths) => {
        process.stdout.write('+');
        for(let i=1; i <= columnWidths[0]; i++) process.stdout.write('-');

        for(let i=1; i < columnWidths.length; i++) {
            process.stdout.write('+');
            for(let j=1; j <= columnWidths[i] - 1; j++) process.stdout.write('-');
        }

        process.stdout.write('+\n');
    }

    const __printId = (value, colWidth) => {
        process.stdout.write('| ' + value + ' ');
        value += '';
        let padLen = colWidth - value.length - 2;
        for(let i=1; i <= padLen; i++) {
            process.stdout.write(' ');
        }
    }

    const __printUuid = (uuid, uuidColWidth) => {
        process.stdout.write('| ' + uuid);
        const rightPadLen = uuidColWidth - uuid.length - 2;
        for(let i=1; i <= rightPadLen; i++) process.stdout.write(' ');
    }

    const _printLine = (line, idColWidth, uuidColWidth) => {
        let keys = Object.keys(line);
        __printId(line[keys[0]], idColWidth);
        for(let i=1; i < keys.length; i++) {
            __printUuid(line[keys[i]], uuidColWidth);
        }
        process.stdout.write('|\n');
    }
    const uuidColWidth = 40;
    const noOfCols = 3;     //Denote the number of internal pipes (i.e |)
    const headers = Object.keys(changes[0]);
    const col1 = headers[0];
    const col1Len = col1.length >= 10 ? col1.length + 2 : 12;
    const lineLength = col1Len + (2 * uuidColWidth) + noOfCols;

    const col2 = 'Initial UUID';
    const col2LeftPadLen = 1;
    const col2RightPadLen = uuidColWidth - col2.length - col2LeftPadLen - 1;

    const col3 = 'New UUID';
    const col3LeftPadLen = 1;
    const col3RightpadLen = uuidColWidth - col3.length - col3LeftPadLen - 1;

    const columnWidths = [col1Len, uuidColWidth, uuidColWidth];
    _printBorder(columnWidths);

    process.stdout.write('| ' + col1 + ' ');
    if(col1.length < 10) {
        let padLen = col1Len - col1.length - 2;
        for(let i=1; i <= padLen; i++) {
            process.stdout.write(' ');
        }
    }

    process.stdout.write('| ' + col2);
    for(let i=1; i <= col2RightPadLen; i++) {
        process.stdout.write(' ');
    }

    process.stdout.write('| ' + col3);
    for(let i=1; i <= col3RightpadLen; i++) {
        process.stdout.write(' ');
    }
    process.stdout.write('|\n');

    _printBorder(columnWidths);
    // End printing header

    // Print values
    changes.forEach(change => {
        _printLine(change, col1Len, uuidColWidth);
    });

    // Print bottom border line
    _printBorder(columnWidths);
}

async function main(srcConn, destConn, dryRun, useTransaction) {
    if(useTransaction === undefined || useTransaction === null) {
        useTransaction = false;
    }

    if(dryRun === undefined || dryRun === null) {
        dryRun = false;
    }

    if(dryRun) {
        useTransaction = true;
    }

    // Make sure the instances are on the same mysql server.
    let commonMsg = `We are going to attempting merging ` +
        `however hoping not to encounter UUID collisions`;
    if(config.source.host !== config.destination.host) {
        let differentInstanceMsg = `The UUID checks can only be run on instances ` +
            `living in the same server.` + commonMsg;
        utils.logDebug(differentInstanceMsg);
        return;
    }

    if(config.source.username !== config.destination.username) {
        let differentUsers = 'Since different users access source & destination ' +
            'db, no attempt is made to check UUID. ' + commonMsg;
        return;
    }

    let excluded = await utils.personIdsToexclude(srcConn);
    let toExclude = '(' + excluded.join(',') + ')';

    let coreTables = [
        { table: 'person', condition: `t1.person_id NOT IN ${toExclude}` },    // Exclude admin person
        { table: 'person_attribute_type' },
        { table: 'person_attribute' },
        { table: 'person_name', condition: `t1.person_id NOT IN ${toExclude}` },   // Exclude admin person
        { table: 'person_address' },
        { table: 'relationship_type' },
        { table: 'relationship' },
        { table: 'patient_identifier_type' },
        { table: 'patient_identifier'},
        { table: 'users', primaryKey: 'user_id', condition: `t1.system_id NOT IN('daemon', 'admin')` },
        { table: 'location' },
        { table: 'provider' },
        { table: 'provider_attribute_type'},
        { table: 'provider_attribute'},
        { table: 'visit' },
        { table: 'visit_type' },
        { table: 'encounter' },
        { table: 'encounter_role' },
        { table: 'encounter_provider'},
        { table: 'obs' },
        { table: 'program' },
        { table: 'program_workflow' },
        { table: 'program_workflow_state' },
        { table: 'patient_state' }
    ];

    let gaacTables = [
        { table: 'gaac' },
        { table: 'gaac_member' },
        { table: 'gaac_affinity_type' },
        { table: 'gaac_reason_leaving_type' }
    ];

    let i = 0;
    let changesMap = new Map();
    let coreTablesDone = false;
    try {
        if(useTransaction) await srcConn.query('START TRANSACTION');

        for(; i < coreTables.length; i++) {
            coreTables[i].connection = srcConn;
            // Do until all UUIDs are unique.
            let changes = await ensureUniqueUuids(coreTables[i]);

            if(changes !== null && changes.length > 0) {
                changesMap.set(coreTables[i].table, changes);
            }

            while (changes !== null && changes.length > 0) {
                changes = await ensureUniqueUuids(coreTables[i]);
                if(changes !== null && changes.length > 0) {
                    changesMap.set(coreTables[i].table, changes);
                }
            }
        }

        if(i === coreTables.length) {
            coreTablesDone = true;
        }
        // Check if gaac table exist.
        let [srcGaacs] = await srcConn.query(`SHOW TABLES LIKE 'gaac%'`);
        let [destGaacs] = await destConn.query(`SHOW TABLES LIKE 'gaac%'`);
        if(srcGaacs.length > 0 && destGaacs.length > 0) {
            i = 0;
            for(; i < gaacTables.length; i++) {
                gaacTables[i].connection = srcConn;
                let changes = null;
                do {
                    // Do until all UUIDs are unique.
                    changes = await ensureUniqueUuids(gaacTables[i]);

                    if(changes !== null && changes.length > 0) {
                        changesMap.set(gaacTables[i].table, changes);
                    }
                } while (changes !== null && changes.length > 0);
            }
        }

        if(changesMap.size > 0) {
            utils.logOk(`The tables with updated UUIDs in source ` +
                                `database(${config.source.location})`)
            changesMap.forEach((changes, table) => {
                utils.logOk(`${table}`);
                prettyPrintUuidChanges(changes);
            });
        }
        else {
            utils.logOk('No UUID conflicts found')
        }

        if(useTransaction) {
            if(dryRun) {
                srcConn.query('ROLLBACK');
                utils.logOk(`Unique UUID checker run successfully, ` +
                                    `(--dry-run mode), changes not committed!`)
            }
            else {
                srcConn.query('COMMIT');
            }
        }
    }
    catch(ex) {
        if(useTransaction) srcConn.query('ROLLBACK');
        let tableName = null;
        if(coreTablesDone) {
            tableName = gaacTables[i].table;
        }
        else {
            tableName = coreTables[i].table;
        }
        utils.logError('Error while ensuring unique UUID for table ' + tableName);
        throw ex;
    }
}

module.exports = main;

// (function testPrettyPrinting() {
//     let changes = [{
//         obs_id: 45678143,
//         initialUuid: '47701676-b8cd-4fa7-bdf1-648b5d72ef55',
//         newUuid: 'e2bcae58-1d5f-11e0-b929-000c29ad1d07'
//     }];
//
//     prettyPrintUuidChanges(changes);
// })();
