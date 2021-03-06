'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const Promise = require('../../promise');
const Utils = require('../../utils');
const debug = Utils.getLogger().debugContext('connection:sqlite');
const dataTypes = require('../../data-types').sqlite;
const sequelizeErrors = require('../../errors');
const parserStore = require('../parserStore')('sqlite');

class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    super(dialect, sequelize);
    this.sequelize = sequelize;
    this.config = sequelize.config;
    this.dialect = dialect;
    this.dialectName = this.sequelize.options.dialect;
    this.connections = {};

    // We attempt to parse file location from a connection uri but we shouldn't match sequelize default host.
    if (this.sequelize.options.host === 'localhost') delete this.sequelize.options.host;

    try {
      if (sequelize.config.dialectModulePath) {
        this.lib = require(sequelize.config.dialectModulePath);
      } else {
        this.lib = require('better-sqlite3');
      }
    } catch (err) {
      if (err.code === 'MODULE_NOT_FOUND') {
        throw new Error('Please install better-sqlite package manually');
      }
      throw err;
    }

    this.refreshTypeParser(dataTypes);
  }

  // Expose this as a method so that the parsing may be updated when the user has added additional, custom types
  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  async getConnection(options) {
    options = options || {};
    options.uuid = options.uuid || 'default';
    options.inMemory = (this.sequelize.options.storage || this.sequelize.options.host || ':memory:') === ':memory:' ? true : false;

    const dialectOptions = this.sequelize.options.dialectOptions;
    options.readWriteMode = dialectOptions && dialectOptions.mode;

    if (this.connections[options.inMemory || options.uuid]) {
      return this.connections[options.inMemory || options.uuid];
    }

    try {
      let connection = new this.lib(
        this.sequelize.options.storage || this.sequelize.options.host || ':memory:',
        {
          readonly: options.readWriteMode || this.lib.OPEN_READWRITE | this.lib.OPEN_CREATE ? true : false, // default mode
          memory: options.inMemory,
        },
      );
      this.connections[options.inMemory || options.uuid] = connection;

      debug(`connection acquired ${options.uuid}`);

      if (this.sequelize.config.password) {
        // Make it possible to define and use password for sqlite encryption plugin like sqlcipher
        connection.pragma('KEY=' + this.sequelize.escape(this.sequelize.config.password));
      }

      if (this.sequelize.options.foreignKeys !== false) {
        // Make it possible to define and use foreign key constraints unless
        // explicitly disallowed. It's still opt-in per relation
        connection.pragma('FOREIGN_KEYS=ON');
      }

      return this.connections[options.inMemory || options.uuid];
    } catch (err) {
      if (err.code === 'SQLITE_CANTOPEN') {
        throw new sequelizeErrors.ConnectionError(err);
      }
      throw new sequelizeErrors.ConnectionError(err);
    }
  }

  releaseConnection(connection, force) {
    if (connection.filename === ':memory:' && force !== true) return;

    if (connection.uuid) {
      connection.close();
      debug(`connection released ${connection.uuid}`);
      delete this.connections[connection.uuid];
    }
  }
}


module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
