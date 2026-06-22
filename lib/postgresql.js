// Copyright IBM Corp. 2013,2020. All Rights Reserved.
// Node module: loopback-connector-postgresql
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/*!
 * PostgreSQL connector for LoopBack
 */
'use strict';
const SG = require('strong-globalize');
const g = SG();

// --- Saipos: logs de diagnostico (keepalive / conexao) controlaveis por env ---
// Log verbose da connection string por conexao nova. Habilitar: SAIPOS_PG_DIAG_LOG=true|1
const diagLogEnabled = () => ['true', '1'].includes(String(process.env.SAIPOS_PG_DIAG_LOG || '').toLowerCase());

// Erros que indicam conexao morta/fechada (alvo do keepalive).
const CONN_ERROR_RE = /server closed the connection unexpectedly|Connection terminated|terminating connection|ECONNRESET|EPIPE/i;

// Dedup do log [PG-OVERRIDE]: 1 linha por application_name por processo. Serve pra
// validar na task que o override subiu ligado (independe de SAIPOS_PG_DIAG_LOG).
const overrideLoggedApps = new Set();

// Loga contexto extra SO para erros de conexao morta, p/ correlacionar com pgcat/infra.
const logConnectionError = (connector, connection, sql, err) => {
  // try/catch externo: o log de diagnostico NUNCA pode lancar e quebrar o fluxo de erro do query.
  try {
    if (!err || !CONN_ERROR_RE.test(err.message || '')) return;
    const s = (connector && connector.settings) || {};
    let backendPID = null;
    try {
      backendPID = connection.processID || (connection.native && connection.native.pid) || null;
    } catch (e) { /* pg-native pode nao expor */ }
    console.error('[PG-CONN-ERROR] ' + JSON.stringify({
      message: err.message,
      host: s.host,
      port: s.port,
      database: s.database,
      user: s.user,
      application_name: s.application_name,
      backendPID: backendPID,
      sql: String(sql || '').slice(0, 200),
      pool: connector && connector.pg
        ? { total: connector.pg.totalCount, idle: connector.pg.idleCount, waiting: connector.pg.waitingCount }
        : null,
      timestamp: new Date().toISOString()
    }));
  } catch (e) { /* best-effort: jamais propagar */ }
};

// ---------------------------------------------------------------------------
// Saipos: TCP keepalive support for the native (libpq) client.
//
// When this connector uses `require('pg').native` (libpq), the keepalive
// settings are silently ignored. pg's ConnectionParameters DOES translate
// `keepAlive`/`keepAliveInitialDelayMillis` into `keepalives`/`keepalives_idle`,
// but `getLibpqConnectionString()` (the only thing the native client passes to
// libpq) never emits them, and pg has no notion of `keepalives_interval` or
// `keepalives_count` at all.
//
// We patch ConnectionParameters BEFORE `pg`'s native client is loaded so it
// receives our subclass from the require cache. The subclass:
//   1. computes the 4 libpq keepalive keywords from the datasource settings
//      (defensively coercing, since LoopBack may hand us env vars as strings);
//   2. appends them to the libpq connection string actually sent to libpq.
//
// Datasource settings consumed (all optional):
//   keepAlive                  -> keepalives           (1/0)
//   keepAliveInitialDelayMillis-> keepalives_idle       (seconds, ms/1000)
//   keepAliveIntervalMillis    -> keepalives_interval   (seconds, ms/1000)
//   keepAliveCount             -> keepalives_count      (probe count)
// ---------------------------------------------------------------------------
const connectionParametersPath = require.resolve('pg/lib/connection-parameters');
const BaseConnectionParameters = require(connectionParametersPath);

class ConnectionParametersWithKeepalive extends BaseConnectionParameters {
  constructor(config) {
    super(config);
    config = config || {};

    // Gate: a logica custom (keepalive + log) so roda se o datasource pedir
    // explicitamente com overrideConnectionParameters=true. Sem isso (ou false),
    // a subclasse age identica ao base -> zero efeito colateral.
    const ov = config.overrideConnectionParameters;
    this._override = (ov === true || ov === 'true' || ov === 1 || ov === '1');
    if (!this._override) return;

    // ausente/null/'' => "nao configurado": nao emite o parametro (libpq usa o default dele).
    // numeros so emitidos se finitos e > 0 (libpq passa o valor direto ao setsockopt;
    // 0 => EINVAL em TCP_KEEPIDLE/TCP_KEEPINTVL/TCP_KEEPCNT).
    const isUnset = (v) => v === undefined || v === null || v === '';
    const asInt = (v) => (isUnset(v) ? NaN : Number(v));

    const keepAlive = config.keepAlive;
    if (!isUnset(keepAlive)) {
      this.keepalives =
        (keepAlive === true || keepAlive === 'true' || keepAlive === 1 || keepAlive === '1') ? 1 : 0;
    }

    const idleSeconds = asInt(config.keepAliveInitialDelayMillis);
    if (Number.isFinite(idleSeconds) && idleSeconds > 0) {
      this.keepalives_idle = Math.floor(idleSeconds / 1000);
    }

    const intervalSeconds = asInt(config.keepAliveIntervalMillis);
    if (Number.isFinite(intervalSeconds) && intervalSeconds > 0) {
      this.keepalives_interval = Math.floor(intervalSeconds / 1000);
    }

    const count = asInt(config.keepAliveCount);
    if (Number.isFinite(count) && count > 0) {
      this.keepalives_count = Math.floor(count);
    }
  }

  getLibpqConnectionString(cb) {
    const self = this;
    super.getLibpqConnectionString(function(err, conString) {
      if (err) return cb(err, null);
      // Sem override: devolve a string base intacta (sem append, sem log).
      if (!self._override) return cb(null, conString);
      const extra = [];
      if (self.keepalives !== undefined) extra.push('keepalives=' + self.keepalives);
      if (self.keepalives_idle !== undefined) extra.push('keepalives_idle=' + self.keepalives_idle);
      if (self.keepalives_interval !== undefined) extra.push('keepalives_interval=' + self.keepalives_interval);
      if (self.keepalives_count !== undefined) extra.push('keepalives_count=' + self.keepalives_count);
      const finalConString = extra.length ? conString + ' ' + extra.join(' ') : conString;
      // Confirmacao do override (1x por application_name, sem env): valida na task que o
      // gate subiu ligado e o que vai pro keepalive. Blindado (nunca quebra a conexao).
      try {
        const appName = self.application_name || 'unknown';
        if (!overrideLoggedApps.has(appName)) {
          overrideLoggedApps.add(appName);
          console.log('[PG-OVERRIDE] app=' + appName + ' override=on ' +
            (extra.length ? extra.join(' ') : 'keepalive=off'));
        }
      } catch (e) { /* best-effort: jamais propagar */ }
      // Log opcional da connection string resolvida (senha mascarada).
      // Controle por env: SAIPOS_PG_DIAG_LOG=true|1 habilita; default desligado.
      if (diagLogEnabled()) {
        // best-effort: um throw aqui nao pode impedir o cb (travaria a conexao).
        try {
          console.log('[KEEPALIVE] libpq connection string:',
            finalConString.replace(/password='(?:\\.|[^'])*'/, "password='****'"));
        } catch (e) { /* ignora */ }
      }
      return cb(null, finalConString);
    });
  }
}

// Replace the cached module so pg's native client (loaded just below via
// `require('pg').native`) picks up our subclass instead of the original.
require.cache[connectionParametersPath].exports = ConnectionParametersWithKeepalive;

const postgresql = require('pg').native;
const SqlConnector = require('loopback-connector').SqlConnector;
const ParameterizedSQL = SqlConnector.ParameterizedSQL;
const util = require('util');
const debug = require('debug')('loopback:connector:postgresql');
const debugData = require('debug')('loopback:connector:postgresql:data');
const debugSort = require('debug')('loopback:connector:postgresql:order');
const Promise = require('bluebird');

console.log("**************************************************")
console.log("pg-native")
console.log("**************************************************")
// Saipos: log de boot (sempre impresso ao carregar o connector) pra validar na task
// que (1) subiu o build certo do fork e (2) o estado da env do log de diagnostico.
console.log('[PG-DIAG] saipos-connector build=2026-06-22 (override-gate + keepalive + pg-override) | SAIPOS_PG_DIAG_LOG=' + (diagLogEnabled() ? 'on' : 'off'))

/**
 *
 * Initialize the PostgreSQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header PostgreSQL.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!postgresql) {
    return;
  }

  const dbSettings = dataSource.settings || {};
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;

  dataSource.connector = new PostgreSQL(postgresql, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    if (dbSettings.lazyConnect) {
      process.nextTick(callback);
    } else {
      dataSource.connecting = true;
      dataSource.connector.connect(callback);
    }
  }
};

/**
 * PostgreSQL connector constructor
 *
 * @param {PostgreSQL} postgresql PostgreSQL node.js binding
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://node-postgres.com/api/client).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the PostgreSQL DB server
 * @property {Number} port The port number of the PostgreSQL DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 * @property {Function | string} [onError] Optional hook to connect to the pg pool 'error' event,
 * or the string 'ignore' to record them with `debug` and otherwise ignore them.
 *
 * @constructor
 */
function PostgreSQL(postgresql, settings) {
  // this.name = 'postgresql';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'postgresql', settings);
  this.clientConfig = settings;
  if (settings.url) {
    // pg-pool doesn't handle string config correctly
    this.clientConfig.connectionString = settings.url;
  }
  this.clientConfig.Promise = Promise;
  this.pg = new postgresql.Pool(this.clientConfig);

  if (settings.onError) {
    if (settings.onError === 'ignore') {
      this.pg.on('error', function(err) {
        debug(err);
      });
    } else {
      this.pg.on('error', settings.onError);
    }
  }

  this.settings = settings;
  debug('Settings %j', settings);
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(PostgreSQL, SqlConnector);

PostgreSQL.prototype.getDefaultSchemaName = function() {
  return 'public';
};

/**
 * Connect to PostgreSQL
 * @callback {Function} [callback] The callback after the connection is established
 */
PostgreSQL.prototype.connect = function(callback) {
  const self = this;
  self.pg.connect(function(err, client, releaseCb) {
    self.client = client;
    process.nextTick(releaseCb);
    callback && callback(err, client);
  });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @param {Object} [options] Options object
 * @callback {Function} [callback] The callback after the SQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[]} data The result from the SQL
 */
PostgreSQL.prototype.executeSQL = function(sql, params, options, callback) {
  const self = this;

  if (params && params.length > 0) {
    debug('SQL: %s\nParameters: %j', sql, params);
  } else {
    debug('SQL: %s', sql);
  }

  function executeWithConnection(connection, releaseCb) {
    connection.query(sql, params, function(err, data) {
      // if(err) console.error(err);
      if (err) {
        debug(err);
        logConnectionError(self, connection, sql, err);
      }
      if (data) debugData('%j', data);
      // Release the connection back to the pool.
      if (releaseCb) releaseCb(err);
      let result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = {affectedRows: data.rowCount, count: data.rowCount};

            if (data.rows)
              result.rows = data.rows;

            break;
          default:
            result = data.rows;
        }
      }
      callback(err ? err : null, result);
    });
  }

  const transaction = options.transaction;
  if (transaction && transaction.connector === this) {
    if (!transaction.connection) {
      return process.nextTick(function() {
        callback(new Error(g.f('Connection does not exist')));
      });
    }
    if (transaction.txId !== transaction.connection.txId) {
      return process.nextTick(function() {
        callback(new Error(g.f('Transaction is not active')));
      });
    }
    debug('Execute SQL within a transaction');
    // Do not release the connection
    executeWithConnection(transaction.connection, null);
  } else {
    self.pg.connect(function(err, connection, releaseCb) {
      if (err) return callback(err);
      executeWithConnection(connection, releaseCb);
    });
  }
};

PostgreSQL.prototype.buildInsertReturning = function(model, data, options) {
  const idColumnNames = [];
  const idNames = this.idNames(model);
  for (let i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

/**
 * Check if id types have a numeric type
 * @param {String} model name
 * @returns {Boolean}
 */
PostgreSQL.prototype.hasOnlyNumericIds = function(model) {
  const cols = this.getModelDefinition(model).properties;
  const idNames = this.idNames(model);
  const numericIds = idNames.filter(function(idName) {
    return cols[idName].type === Number;
  });

  return numericIds.length == idNames.length;
};

/**
 * Get default find sort policy
 * @param model
 */
PostgreSQL.prototype.getDefaultIdSortPolicy = function(model) {
  const modelClass = this._models[model];

  if (modelClass.settings.hasOwnProperty('defaultIdSort')) {
    return modelClass.settings.defaultIdSort;
  }

  if (this.settings.hasOwnProperty('defaultIdSort')) {
    return this.settings.defaultIdSort;
  }

  return null;
};

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: ...}
 */
PostgreSQL.prototype.buildSelect = function(model, filter) {
  let sortById;

  const sortPolicy = this.getDefaultIdSortPolicy(model);

  switch (sortPolicy) {
    case 'numericIdOnly':
      sortById = this.hasOnlyNumericIds(model);
      break;
    case false:
      sortById = false;
      break;
    default:
      sortById = true;
      break;
  }

  debugSort(model, 'sort policy:', sortPolicy, sortById);

  if (sortById && !filter.order) {
    const idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  let selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter) +
    ' FROM ' + this.tableEscaped(model));

  if (filter) {
    if (filter.where) {
      const whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter.order));
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter,
      );
    }
  }
  return this.parameterize(selectStmt);
};

PostgreSQL.prototype.buildInsertDefaultValues = function(model, data, options) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// postgresql 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-postgresql/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 PostgreSQL.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

PostgreSQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  const type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
      val === 't' || val === '1');
    }
  } else if (prop && type === 'GeoPoint' || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      const point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1],
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now pg driver converts point to {x: lng, y: lat}
      return {
        lng: val.x,
        lat: val.y,
      };
    } else {
      return val;
    }
  } else {
    return val;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
PostgreSQL.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  // PostgreSQL default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  let escaped = '"';
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  let hasBackslash = false;
  let escaped = '\'';
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*
 * Check if a value is attempting to use nested json keys
 * @param {String} property The property being queried from where clause
 * @returns {Boolean} True of the property contains dots for nested json
 */
function isNested(property) {
  return property.split('.').length > 1;
}

/*
 * Overwrite the loopback-connector column escape
 * to allow querying nested json keys
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The escaped column name, or column with nested keys for deep json columns
 */
PostgreSQL.prototype.columnEscaped = function(model, property) {
  if (isNested(property)) {
    // Convert column to PostgreSQL json style query: "model"->>'val'
    const self = this;
    return property
      .split('.')
      .map(function(val, idx) { return (idx === 0 ? self.columnEscaped(model, val) : escapeLiteral(val)); })
      .reduce(function(prev, next, idx, arr) {
        return idx == 0 ? next : idx < arr.length - 1 ? prev + '->' + next : prev + '->>' + next;
      });
  } else {
    return this.escapeName(this.column(model, property));
  }
};

/*!
 * Escape the name for PostgreSQL DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
PostgreSQL.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

PostgreSQL.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  // Can't send functions, objects, arrays
  if (typeof value === 'object' || typeof value === 'function') {
    return null;
  }
  return value;
};

PostgreSQL.prototype.tableEscaped = function(model) {
  const schema = this.schema(model) || 'public';
  return this.escapeName(schema) + '.' +
    this.escapeName(this.table(model));
};

function buildLimit(limit, offset) {
  const clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

PostgreSQL.prototype.applyPagination = function(model, stmt, filter) {
  const limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

PostgreSQL.prototype.buildExpression = function(columnName, operator,
  operatorValue, propertyDefinition) {
  switch (operator) {
    case 'like':
      return new ParameterizedSQL(columnName + "::TEXT LIKE ? ESCAPE E'\\\\'",
        [operatorValue]);
    case 'ilike':
      return new ParameterizedSQL(columnName + "::TEXT ILIKE ? ESCAPE E'\\\\'",
        [operatorValue]);
    case 'nlike':
      return new ParameterizedSQL(columnName + "::TEXT NOT LIKE ? ESCAPE E'\\\\'",
        [operatorValue]);
    case 'nilike':
      return new ParameterizedSQL(columnName + "::TEXT NOT ILIKE ? ESCAPE E'\\\\'",
        [operatorValue]);
    case 'regexp':
      if (operatorValue.global)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`m`}} flag');

      const regexOperator = operatorValue.ignoreCase ? ' ~* ?' : ' ~ ?';
      return new ParameterizedSQL(columnName + regexOperator,
        [operatorValue.source]);
    case 'contains':
      return new ParameterizedSQL(columnName + ' @> array[' + operatorValue.map((v) => `'${v}'`) + ']::'
        + propertyDefinition.postgresql.dataType);
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper('buildExpression', columnName, operator,
        operatorValue, propertyDefinition);
  }
};

/**
 * Disconnect from PostgreSQL
 * @param {Function} [cb] The callback function
 */
PostgreSQL.prototype.disconnect = function disconnect(cb) {
  if (this.pg) {
    debug('Disconnecting from ' + this.settings.hostname);
    const pg = this.pg;
    this.pg = null;
    pg.end(); // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

PostgreSQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', [], cb);
};

PostgreSQL.prototype.getInsertedId = function(model, info) {
  const idColName = this.idColumn(model);
  let idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} where An object for the where conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
PostgreSQL.prototype.buildWhere = function(model, where) {
  const whereClause = this._buildWhere(model, where);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};

/**
 * @private
 * @param model
 * @param where
 * @returns {ParameterizedSQL}
 */
PostgreSQL.prototype._buildWhere = function(model, where) {
  let columnValue, sqlExp;
  if (!where) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const self = this;
  const props = self.getModelDefinition(model).properties;

  const whereStmts = [];
  for (const key in where) {
    const stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      const branches = [];
      let branchParams = [];
      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (let i = 0, n = clauses.length; i < n; i++) {
          const stmtForClause = self._buildWhere(model, clauses[i]);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: '(' + branches.join(' ' + key.toUpperCase() + ' ') + ')',
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    let p = props[key];

    if (p == null && isNested(key)) {
      // See if we are querying nested json
      p = props[key.split('.')[0]];
    }

    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }
    // eslint-disable one-var
    let expression = where[key];
    const columnName = self.columnEscaped(model, key);
    // eslint-enable one-var
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && expression.constructor === Object) {
      const operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (let j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j], true));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression, true));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          const v1 = columnValue[0] === undefined ? null : columnValue[0];
          const v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression, true);
      }
      sqlExp = self.buildExpression(columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          if (p.type.name === 'GeoPoint')
            stmt.merge(columnName + '~=').merge(columnValue);
          else
            stmt.merge(columnName + '=').merge(columnValue);
        } else {
          stmt.merge({
            sql: columnName + '=?',
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  let params = [];
  const sqls = [];
  for (let k = 0, s = whereStmts.length; k < s; k++) {
    sqls.push(whereStmts[k].sql);
    params = params.concat(whereStmts[k].params);
  }
  const whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @param {boolean} isWhereClause
 * @returns {*} The escaped value of DB column
 */
PostgreSQL.prototype.toColumnValue = function(prop, val, isWhereClause) {
  if (val == null) {
    // PostgreSQL complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    // Do not return 'DEFAULT' for id field in where clause
    if (prop.autoIncrement || (prop.id && !isWhereClause)) {
      return new ParameterizedSQL('DEFAULT');
    } else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    const iso = val.toISOString();

    // Pass in date as UTC and make sure Postgresql stores using UTC timezone
    return new ParameterizedSQL({
      sql: '?::TIMESTAMP WITH TIME ZONE',
      params: [iso],
    });
  }

  // PostgreSQL support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint' || prop.type.name === 'Point') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      // Postgres point is point(lng, lat)
      params: [val.lng, val.lat],
    });
  }

  if (Array.isArray(prop.type)) {
    // There is two possible cases for the type of "val" as well as two cases for dataType
    const isArrayDataType = prop.postgresql && prop.postgresql.dataType === 'varchar[]';
    if (Array.isArray(val)) {
      if (isArrayDataType) {
        return val;
      } else {
        return JSON.stringify(val);
      }
    } else {
      if (isArrayDataType) {
        return JSON.parse(val);
      } else {
        return val;
      }
    }
  }

  return val;
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error(g.f('{{Placeholder}} for identifiers is not supported'));
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

PostgreSQL.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.affectedRows;
};

require('./discovery')(PostgreSQL);
require('./migration')(PostgreSQL);
require('./transaction')(PostgreSQL);
