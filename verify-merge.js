'use strict';
const connection = require('./connection').connection;
const utils = require('./utils');
const config = require('./config');

function printUnmovedRecords(records, tableName) {
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
        __printId(line[`${tableName}_id`], idColWidth);
        __printUuid(line['uuid'], uuidColWidth);
        process.stdout.write('|\n');
    }
    const uuidColWidth = 40;
    const noOfCols = 2;     //Denote the number of internal pipes (i.e |)
    const col1 = `${tableName}_id`;
    const col1Len = col1.length >= 10 ? col1.length + 2 : 12;
    const lineLength = col1Len + (2 * uuidColWidth) + noOfCols;

    const col2 = 'UUID';
    const col2LeftPadLen = 1;
    const col2RightPadLen = uuidColWidth - col2.length - col2LeftPadLen - 1;

    const columnWidths = [col1Len, uuidColWidth];
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

    process.stdout.write('|\n');

    _printBorder(columnWidths);
    // End printing header

    // Print values
    records.forEach(row => {
        _printLine(row, col1Len, uuidColWidth);
    });

    // Print bottom border line
    _printBorder(columnWidths);
}

async function verifyTable(connection, tableName, condition) {
    let query = `SELECT * FROM ${config.source.openmrsDb}.${tableName} WHERE `
    
    if(condition) {
        query += condition + ' AND ';
    }
    
    query += `uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.${tableName})`;
    
    try {
        let [result] = await connection.query(query);
        if(result.length > 0) {
            utils.logInfo(`The following records from ${tableName} were not moved`);
            printUnmovedRecords(result, tableName);
        } else {
            utils.logInfo(`All records for table ${tableName} moved successfully`);
        }
    } catch(ex) {
        utils.logError(`SQL Statement during merge verification is: ${query}`);
        throw ex;
    }
}

// Works only if the databases are housed in the same mysql instance.
async function main() {
    let srcConn = await connection(config.source);
    let destConn = await connection(config.destination);
    let tables = ['person', 'person_attribute', 'person_name', 'person_address', 
                'relationship', 'patient_identifier', 'visit', 'encounter',
                'provider', 'program_workflow', 'patient_state', 'obs'];

    utils.logInfo('Checking if gaac module tables exists in source');
    let [r] = await srcConn.query(`SHOW TABLES LIKE 'gaac%'`);

    if(r.length > 0) {
        Array.prototype.push.call(tables, 'gaac', 'gaac_member');
    } else {
        utils.logDebug('No gaac tables in the source database')
    }
    
    for(let i=0; i < tables.length; i++) {
        let condition = null;
        if(tables[i] === 'person' || tables[i] === 'person_name' || tables[i] === 'person_address') {
            let q = `SELECT * FROM users WHERE system_id IN ('admin','daemon')`;
            let [r] = await srcConn.query(q);
            if(r.length > 0) {
                condition = 'person_id NOT IN (';
                r.forEach(row => {
                    condition += row['person_id'] + ', ';
                });
                condition = condition.substring(0, condition.lastIndexOf(','));
                condition += ')';
            }
        }
        await verifyTable(srcConn, tables[i], condition);
    }

    // This needed when run as a file because the app will hang due to asyn/wait calls.
    process.exit(0);
}

//Run the function.
main();

module.exports = main;


// Test printing with dummy data.
// printUnmovedRecords([{'person_id': 12, uuid:'1123-dkfdkd-4o433o-kdlsdl'}], 'person');