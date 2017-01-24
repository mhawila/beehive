(function() {
  'use strict';
  const connection = require('./connection').connection;
  const prepare = require('./preparation').prepare;
  const utils = require('./utils');
  const logTime = utils.logTime;
  const log = utils.logLog;
  const logError = utils.logError;
  const config = require('./config');

  if(config.source.location === undefined) {
    logError('Error: Please specify unique source.location in config.json file');
    process.exit(1);
  }

  let srcConn = null;
  let destConn = null;
  try {
    srcConn = await connection(config.source);
    destConn = await connection(config.destination);

    console.log(logTime(), ': Starting migration...');
    await prepare(destConn, config.source.location);
  }
  catch(ex) {

  }

})();
