'use strict';
const utils = require('./utils');
const prettyPrintRows = require('./display-utils').prettyPrintRows;

async function checkForeignKeys(connection, params) {
    let query = `SELECT ${params.primaryKey},${params.foreignKey} ` +
        `FROM ${params.table} AS t1 WHERE t1.${params.foreignKey} IS NOT NULL ` +
        `AND t1.${params.foreignKey} > 0 ` +
        `AND NOT EXISTS (SELECT 1 FROM ${params.foreignKeyTable} AS t2 ` +
        `WHERE t1.${params.foreignKey} = t2.${params.reference})`;

    utils.logDebug(`Query to check ${params.table}.${params.foreignKey}` +
            ` foreign key inconsistencies:`);
    utils.logDebug(query);

    try {
        let [result] = await connection.query(query);
        return result;
    }
    catch(ex) {
        utils.logError(`SQL statement during error:`, query);
        throw ex;
    }
}

async function doWork(connection, schema) {
    const resultsMap = new Map();

    let tables = [
        'person',
        'person_name',
        'person_attribute',
        'person_address',
        'relationship',
        'patient',
        'patient_identifier',
        'visit',
        'encounter',
        'encounter_provider',
        'obs',
        'patient_state',
        'users',
        'person_attribute_type',
        'location',
        'provider',
        'provider_attribute_type',
        'visit_type',
        'relationship_type',
        'patient_identifier_type',
        'program',
        'patient_program',
        'patient_state',
        'program_workflow',
        'program_workflow_state',
        'gaac',
        'gaac_member',
        'gaac_affinity_type',
        'gaac_reason_leaving_type'
    ];

    //Get all tables with associated foreignKey
    let inClauseList = tables.map(table => {
        return utils.stringValue(table);
    }).join(', ');

    let dbString = utils.stringValue(schema);
    let query = `SELECT table_name, column_name, ` +
        `referenced_table_name, referenced_column_name, ` +
            `(SELECT column_name FROM information_schema.columns WHERE ` +
            `table_schema = ${dbString} AND table_name = t.table_name ` +
            `AND column_key='PRI') AS primary_key ` +
        `FROM information_schema.key_column_usage AS t ` +
        `WHERE table_schema = ${dbString} ` +
        `AND referenced_table_name IS NOT NULL ` +
        `AND referenced_column_name IS NOT NULL ` +
        `AND table_name IN (${inClauseList})`;

    utils.logDebug(`Query to get tables for foreign key integrity checks`);
    utils.logDebug(query);

    let [tablesInfo] = await connection.query(query);

    //Build results for each entry in the table info.
    for(let i=0; i < tablesInfo.length; i++) {
        let params = {
            table: tablesInfo[i]['table_name'],
            primaryKey: tablesInfo[i]['primary_key'],
            foreignKey: tablesInfo[i]['column_name'],
            foreignKeyTable: tablesInfo[i]['referenced_table_name'],
            reference: tablesInfo[i]['referenced_column_name']
        };
        let results = await checkForeignKeys(connection, params);

        if(results.length>0) {
            let transformed = results.map(result => {
                return Object.assign({}, params, {
                    primaryKeyValue: result[tablesInfo[i]['primary_key']],
                    foreignKeyValue: result[tablesInfo[i]['column_name']]
                });
            });
            let existing = resultsMap[tablesInfo[i]['table_name']];
            if(existing) {
                //There is already something there.
                // This below kills the stack hence has to be replaced.
                // Array.prototype.push.apply(existing, transformed);
                for(let i=0; i < transformed.length; i++) {
                    existing.push(transformed[i]);
                }
                resultsMap[tablesInfo[i]['table_name']] =  existing;
            }
            else {
                resultsMap[tablesInfo[i]['table_name']] =  transformed;
            }
        }
    }

    return resultsMap;
}

async function main(connection, schema) {
    let resultsMap = await doWork(connection, schema);

    if(resultsMap.size > 0) {
        utils.logError(`Below tables have orphaned foreign key values in ` +
            `source database`);
        let headerColumns = {
            table: 'Table',
            primaryKey: 'PRI Key Name',
            foreignKey: 'FRG Key Name',
            foreignKeyTable: 'REF Table',
            reference: 'REF Field',
            primaryKeyValue: 'PRI Key Value',
            foreignKeyValue: 'Orphaned FRG Key Value'
        }
        resultsMap.forEach((results, table) => {
            utils.logError(`${table} table problematic records`);
            prettyPrintRows(results, headerColumns);
            console.log();  //Blank line
        });
        throw new Error(`Cannot proceed because of the data inconsistencies, ` +
            `Please fix the errors before proceeding`);
    }
}

module.exports = main;

// (async function test() {
//     const connection = require('./connection').connection;
//     const config = require('./config');
//
//     const conn = await connection(config.source);
//     try {
//         await main(conn, 'openmrs');
//     }
//     catch(trouble) {
//       console.error('troubles', trouble);
//       process.exit(0);
//     }
//     finally {
//       if(conn) conn.end();
//     }
// })();
