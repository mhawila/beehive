'use strict';
const moment = require('moment');
const mysql = require('mysql2');
const uuidGenerator = require('uuid/v1');
const config = require('./config');

if (config.batchSize === undefined) {
    config.batchSize = 500;
};

let getNextAutoIncrementId = async function(connection, table) {
    if (arguments.length < 2) {
        throw new Error('This utility function expects connection & table in that order');
    }

    let query = `SELECT AUTO_INCREMENT as next_auto FROM information_schema.tables
  WHERE table_name=? and table_schema=database()`;
    try {
        let [r, f] = await connection.execute(query, [table]);
        return r[0]['next_auto'];
    } catch (trouble) {
        console.error('An error occured while fetching next auto increment for table ' +
            table, ':\n', trouble);
        throw trouble;
    }
};

let getCount = async function(connection, table, condition) {
    let countQuery = `SELECT count(*) as table_count FROM ${table}`;
    if (condition) {
        countQuery += ' WHERE ' + condition;
    }

    let [results] = await connection.query(countQuery);
    return results[0]['table_count'];
}

let formatDate = function(d, format) {
    //some how undefined is parsed by moment!!!!
    if (d == undefined) return null;
    if (moment(d).isValid()) {
        return moment(d).format('YYYY-MM-DD HH:mm:ss');
    }
    return null;
};

let logTime = function() {
    return formatDate(Date.now());
}

let stringValue = function(value) {
    return mysql.escape(value);
}

function uuid(existing) {
    if (config.generateNewUuids) return `'${uuidGenerator()}'`;
    return `'${existing}'`;
}

let consolidateTableRecords = async function(srcConn, destConn, table,
    comparisonColumn, idColumn, idMap, insertQueryPrepareFunction) {
    let query = `SELECT * FROM ${table}`;
    let [srcRecords] = await srcConn.query(query);
    let [destRecords] = await destConn.query(query);

    let missingInDest = [];
    srcRecords.forEach(srcRecord => {
        let match = destRecords.find(destRecord => {
            return srcRecord[comparisonColumn] === destRecord[comparisonColumn];
        });

        if (match !== undefined && match !== null) {
            idMap.set(srcRecord[idColumn],
                match[idColumn]);
        } else {
            missingInDest.push(srcRecord);
        }
    });

    if (missingInDest.length > 0) {
        let sql = null;
        try {
            let nextDestId = await getNextAutoIncrementId(destConn, table);

            [sql] = prepareIdentifierTypeInsert(missingInDest, nextDestId);
            logDebug(`${table} insert statement:\n ${sql}`);
            let [result] = await destConn.query(sql);
            return result.affectedRows;
        }
        catch(ex) {
            logError(`An error occured while consolidating ${table} records`);
            if(sql) {
                logError('Insert statement during error');
                logError(sql);
            }
            throw ex;
        }
    }
    else {
        return 0;
    }
}

/**
 * Utility function that moves all table records in config.batchSize batches
 * @param srcConn
 * @param destConn
 * @param tableName:String Name of table whose records are to be moved.
 * @param orderColumn:String Name of the column to order records with.
 * @param insertQueryPrepareFunction: function prepares the insert query
 * @return count of records moved. (or a promise that resolves to count)
 */
let moveAllTableRecords = async function(srcConn, destConn, tableName, orderColumn,
    insertQueryPrepareFunction) {
    // Get the count to be pushed
    let countToMove = await getCount(srcConn, tableName);
    let nextAutoIncr = await getNextAutoIncrementId(destConn, tableName);

    let fetchQuery = `SELECT * FROM ${tableName} ORDER by ${orderColumn} LIMIT `;
    let start = 0;
    let temp = countToMove;
    let moved = 0;
    let queryLogged = false;
    let query = null;
    let [q, nextId] = [null, -1];
    try {
        while (temp % config.batchSize > 0) {
            query = fetchQuery;
            if (Math.floor(temp / config.batchSize) > 0) {
                // moved += config.batchSize;
                query += start + ', ' + config.batchSize;
                temp -= config.batchSize;
            } else {
                // moved += temp;
                query += start + ', ' + temp;
                temp = 0;
            }
            start += config.batchSize;
            let [r] = await srcConn.query(query);
            [q, nextId] = insertQueryPrepareFunction.call(null, r, nextAutoIncr);

            if (!queryLogged) {
                logDebug(`${tableName} insert statement:\n`, shortenInsertStatement(q));
                queryLogged = true;
            }

            if(q) {
                [r] = await destConn.query(q);
                moved += r.affectedRows;
            }

            nextAutoIncr = nextId;
        }
        return moved;
    }
    catch(ex) {
        logError(`An error occured when moving ${tableName} records`);
        if(q) {
            logError('Select statement:', query);
            logError('Insert statement during error');
            logError(q);
        }
        throw ex;
    }
}

let logError = function(...args) {
    args.splice(0, 0, "\x1b[31m");
    console.error.apply(null, args);
}

let logOk = function(...args) {
    args.splice(0, 0, "\x1b[32m");
    console.log.apply(null, args);
}

let logDebug = function(...args) {
    args.splice(0, 0, "\x1b[33m");
    if (config.debug) {
        console.log.apply(null, args);
    }
}

let logInfo = function(...args) {
    args.splice(0, 0, "\x1b[37m");
    console.log.apply(null, args);
}

let shortenInsertStatement = function(statement) {
    let charcount = 700;
    if(statement === undefined || statement === null) statement;
    let valuesIndex = statement.indexOf('VALUES');
    if(valuesIndex == -1)return statement
    if(statement.substring(valuesIndex).length <= charcount) return statement;

    let lastParenth = statement.lastIndexOf(')', valuesIndex + charcount);
    return statement.substring(0, lastParenth + 1) + '...';
}

module.exports = {
    getNextAutoIncrementId: getNextAutoIncrementId,
    getCount: getCount,
    stringValue: stringValue,
    moveAllTableRecords: moveAllTableRecords,
    formatDate: formatDate,
    logTime: logTime,
    logOk: logOk,
    logError: logError,
    logDebug: logDebug,
    logInfo: logInfo,
    uuid: uuid,
    shortenInsert: shortenInsertStatement,
    consolidateRecords: consolidateTableRecords
};
