'use strict'
const mysql = require('mysql2/promise');
const MYSQL_DEFAULT_PORT = 3306;

let connection = async function(hostInfo) {
    return await mysql.createConnection({
        host: hostInfo.host,
        port: hostInfo.port || MYSQL_DEFAULT_PORT,
        user: hostInfo.username,
        password: hostInfo.password,
        database: hostInfo.openmrsDb || 'openmrs',
    });
};

module.exports = {
    connection: connection
};
