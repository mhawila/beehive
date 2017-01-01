const config = require('./config');
const mysql = require('mysql2/promise');
const MYSQL_DEFAULT_PORT = 3306;

console.log('Creating source connection...');

// let srcConn = await mysql.createConnection({
//     host: config.source.host,
//     port: config.source.port || MYSQL_DEFAULT_PORT,
//     user: config.source.username,
//     password: config.source.password,
//     database: config.source.openmrsDb || 'openmrs',
// });
//
// let count = await srcConn.execute('SELECT count(*) from person limit 10');
//
// console.log('This is the count' + count);

(async function() {

  try {
 srcConn = await mysql.createConnection({
    host: config.source.host,
    port: config.source.port || MYSQL_DEFAULT_PORT,
    user: config.source.username,
    password: config.source.password,
    database: config.source.openmrsDb || 'openmrs',
});

let [results, fields] = await srcConn.execute('SELECT * from person limit 10');

console.log('This is the results:', JSON.stringify(results,null,2));
// console.log('Fields:', fields);
}
catch(err) {
  console.error(err);
}
}())
.then(()=>{});
