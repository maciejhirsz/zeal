"use strict";

var mysql = require('mysql');

var Builder = (function () {
    /**
     * @param {string|null} table
     * @param {string|undefined} query
     * @param {Object|undefined} values
     * @constructor
     */
    function Builder(table, query, values) {
        this._table = table;
        this._query = query;
        this._conditions = values;
    }

    /**
     * @returns {string}
     */
    function buildSelect() {
        if (this._select == null) return '*';

        return this._select.map(function (column) {
            if (column === '*') return column;
            reutrn mysql.escapeId(column);
        }).join(', ');
    }

    /**
     * @returns {string}
     */
    function buildConditions() {
        var conditions = this._conditions;
        return Object.keys(conditions).map(function (column) {
            var value = conditions[column];
            column = mysql.escapeId(column);
            if (value === null) {
                return column + ' IS NULL';
            } else if (Array.isArray(value)) {
                return column + ' IN ' + zeal.escape(value);
            } else {
                return column + ' = ' + zeal.escape(value);
            }
        }).join(this._conditionGlue);
    }

    /**
     * @returns {string}
     */
    function buildData() {
        var data = this._data;
        return Object.keys(data).map(function (column) {
            return mysql.escape(column) + ' = ' + zeal.escape(data[column]);
        }).join(', ');
    }

    /**
     * @returns {string}
     */
    function buildLimits() {
        return this._limits.map(Number).join(', ');
    }

    /**
     * called from within proto.insert if this._data is an array
     *
     * @returns {Promise}
     */
    function insertMany() {
        if (this._data.length === 0) return Promise.resovle(false);

        var columns = Object.keys(this._data[0]);

        var columnsQuery = columns.map(mysql.escapeId).join(', '),
            query        = 'INSERT ' + (this._ignore ? 'IGNORE ' : '') + 'INTO ' + mysql.escapeId(this._table) + ' (' + columnsQuery + ') VALUES ';

        query += this._data.map(function (row) {
            return '(' + columns.map(function (column) {
                    return zeal.escape(row[column]);
                }).join(',') + ')';
        });

        return zeal.execute(query);
    }

    /* -- public chaining methods -- */

    var proto        = Builder.prototype;

    /**
     * @param {...string}
     * @returns {Builder}
     */
    proto.select     = function select() {
        var fields = Array.prototype.slice.call(arguments);
        if (fields.length === 1 && Array.isArray(fields[0])) fields = fields[0];

        this._select = fields;

        return this;
    };

    /**
     * @param {boolean} enable
     * @returns {Builder}
     */
    proto.or         = function or(enable) {
        this._conditionGlue = enable ? ' OR ' : ' AND ';

        return this;
    };

    /**
     * @param {Object} conditions
     * @returns {Builder}
     */
    proto.conditions = function conditions(conditions) {
        this._conditions = conditions;
        return this;
    };

    /**
     * @param {Object} data
     * @returns {Builder}
     */
    proto.data       = function data(data) {
        this._data = data;
        return this;
    };

    /**
     * @param {string} column
     * @returns {Builder}
     */
    proto.asc        = function asc(column) {
        if (this._order == null) this._order = [];
        this._order.push(mysql.escapeId(column) + ' ASC');
        return this;
    };

    /**
     * @param {string} column
     * @returns {Builder}
     */
    proto.desc       = function desc(column) {
        if (this._order == null) this._order = [];
        this._order.push(mysql.escapeId(column) + ' DESC');
        return this;
    };

    /**
     * @param {...number}
     * @returns {Builder}
     */
    proto.limit      = function limit() {
        this._limits = Array.prototype.slice.call(arguments);
        return this;
    };

    /**
     * @param {boolean} value
     * @returns {Builder}
     */
    proto.ignore     = function ignore(value) {
        this._ignore = value;
        return this;
    };

    /**
     * @returns {Promise}
     */
    proto.update     = function update() {
        if (this._data == null && this._query == null) {
            return Promise.reject(new Error("SQL: Missing data for an UPDATE query!"));
        }

        var query      = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'UPDATE `' + this._table + '` SET ';
            query += buildData.call(this);
            if (conditions != null) query += " WHERE " + buildConditions.call(this);
            if (this._limit != null) query += " LIMIT " + buildLimits.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    };

    /**
     * @returns {Promise}
     */
    proto.upsert     = function upsert() {
        this._upsert = true;
        return this.insert();
    };

    /**
     * @returns {Promise}
     */
    proto.insert     = function insert() {
        var query = this._query;

        if (query == null) {
            if (Array.isArray(this._data)) return insertMany.call(this);

            query = 'INSERT ' + (this._ignore ? 'IGNORE ' : '') + 'INTO ' + mysql.escapeId(this._table) + ' SET ';

            var queryData = buildData.call(this);

            query += queryData;

            if (this._upsert) query += ' ON DUPLICATE KEY UPDATE ' + queryData;
        }

        return zeal.execute(query).then(function (result) {
            return result.insertId == null ? true : result.insertId;
        });
    };

    /**
     * @returns {Promise}
     */
    proto.erase      = function erase() {
        var query      = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'DELETE FROM ' + mysql.escapeId(this._table);
            if (conditions != null) query += ' WHERE ' + buildConditions.call(this);
            if (this._limit != null) query += ' LIMIT ' + buildLimits.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    };

    /**
     * @returns {Promise}
     */
    proto.truncate   = function truncate() {
        var query = 'TRUNCATE TABLE ' + mysql.escapeId(this._table);

        return zeal.execute(query);
    };

    /**
     * @returns {Promise}
     */
    proto.one        = function one() {
        return this.many().then(function (rows) {
            return rows.shift || null;
        });
    };

    /**
     * @returns {Promise}
     */
    proto.many       = function many() {
        var query      = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'SELECT ' + buildSelect.call(this) + ' FROM ' + mysql.escapeId(this._table);

            if (conditions != null) query += ' WHERE ' + buildConditions.call(this);
            if (this._order != null) query += ' ORDER BY ' + this._order.join(', ');
            if (this._limit != null) query += ' LIMIT ' + buildLimits.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    };

    /**
     * @returns {Promise}
     */
    proto.field      = function field() {
        return this.one().then(function (row) {
            if (row == null) return null;
            var field = Object.keys(row)[0];
            return row[field] || null;
        });
    };

    /**
     * @returns {Promise}
     */
    proto.column     = function column() {
        return this.many().then(function (rows) {
            if (rows.length === 0) return [];
            var field = Object.keys(rows[0])[0];
            return rows.map(function (row) {
                return row[field];
            });
        });
    };

    return Builder;
})();

var pool;

function queryFormat(query, values) {
    if (values == null) return query;

    return query.replace(/\:(\w+)/gm, function (txt, key) {
        if (values.hasOwnProperty(key)) return zeal.escape(values[key]);
        return txt;
    });
}

var zeal = module.exports = {

    /**
     * @param {Object} options, look at mysql module config for pools
     */
    configure : function configure(options) {
        if (pool) throw new Error('Can only call zeal.configure once!');

        options.queryFormat = queryFormat;

        pool = mysql.createPool(options);
    },

    /**
     * @param {*} value
     * @returns {string}
     */
    escape : function escape(value) {
        if (Array.isArray(value)) return '(' + value.map(mysql.escape).join(',') + ')';
        return mysql.escape(value);
    },

    /**
     * @param {string} table
     */
    table : function table(table) {
        return new Builder(table);
    },

    /**
     * @param {string} query
     * @param {Object|undefined|null} values
     */
    query : function query(query, values) {
        return new Builder(null, query, values);
    },

    /**
     * @param query
     * @param values
     * @returns {Promise}
     */
    execute : function execute(query, values) {
        return new Promise(function (resolve, reject) {
            pool.query(query, values, function (err, result) {
                if (err) return reject(err);

                resolve(result);
            });
        });
    }
};