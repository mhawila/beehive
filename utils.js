'use strict';
const moment = require('moment');

let getNextAutoIncrementId = async function(connection, table) {
  if(arguments.length < 2) {
    throw new Error('This utility function expects connection & table in that order');
  }

  let query = `SELECT AUTO_INCREMENT as next_auto FROM information_schema.tables
  WHERE table_name=? and table_schema=database()`;
  try {
    let [r, f] = await connection.execute(query, [table]);
    return r[0]['next_auto'];
  }
  catch(trouble) {
    console.error('An error occured while fetching next auto increment for' +
    table, trouble);
  }
};

let formatDate = function(d, format) {
  //some how undefined is parsed by moment!!!!
  if(d==undefined) return null;
  if(moment(d).isValid()) {
    return moment(d).format('YYYY-MM-DD HH:mm:ss');
  }
  return null;
};

module.exports = {
  getNextAutoIncrementId: getNextAutoIncrementId,
  formatDate: formatDate
};
