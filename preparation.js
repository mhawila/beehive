(function() {
  async function _createTables(connection) {
    let personMapTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_personmap('
          + 'source VARCHAR(50),'
          + 'src_person_id INT(11) NOT NULL,'
          + 'dest_person_id INT(11) NOT NULL,'
          + 'UNIQUE(source, src_person_id)'
          + ')';

    let userMapTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_usermap('
          + 'source VARCHAR(50),'
          + 'src_user_id INT(11) NOT NULL,'
          + 'dest_user_id INT(11) NOT NULL,'
          + 'UNIQUE(source, src_user_id)'
          + ')';

    let progressTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_progress('
      + 'id INT(11) AUTO_INCREMENT PRIMARY KEY,'
      + 'source VARCHAR(50) NOT NULL,'
      + 'phase VARCHAR(50) NOT NULL,'
      + 'status TINYINT,'
      + 'time_finished TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
      + ')';

      let tables = [progressTable, personMapTable, userMapTable];
      try {
        //Create these tables.
        for(let i=0; i < tables.length; i++) {
          await connection.query(tables[i]);
        };
      }
      catch(ex) {
        console.error('Error during merge preparations');
        throw ex;
      }
  }

  module.exports = {
    prepare: _createTables,
  };
})();
