import mysql from 'mysql';

var Builder = (function(){
    function Builder(table, query, values) {
        this._table = table;
        this._query = query;
        this._conditions = values;
    }

    /* -- private helpers -- */

    function buildSelect() {
        if (this._select == null) return '*';

        return this._select.map(mysql.escapeId).join(', ');
    }

    function buildConditions() {
        return Object.keys(this._conditions).map(function(column) {
            var value = this._conditions[column];
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

    function buildData() {
        return Object.keys(this._data).map(function(column) {
            return mysql.escape(column) + ' = ' + zeal.escape(this._data[column]);
        });
    }

    function buildLimit() {
        return this._limit.map(Number).join(', ');
    }

    /* -- public chaining methods -- */

    var proto = Builder.prototype;

    proto.select = function select() {
        var fields = Array.prototype.slice.call(arguments);
        if (fields.length === 1 && Array.isArray(fields[0])) fields = fields[0];

        this._select = fields;

        return this;
    }

    proto.or = function or(enable) {
        this._conditionGlue = enable ? ' OR ' : ' AND ';

        return this;
    }

    proto.conditions = function conditions(conditions) {
        this._conditions = conditions;
        return this;
    }

    proto.data = function data(data) {
        this._data = data;
        return this;
    }

    proto.asc = function asc(column) {
        if (this._order == null) this._order = [];
        this._order.push(mysql.escapeId(column) + ' ASC');
        return this;
    }

    proto.desc = function desc(column) {
        if (this._order == null) this._order = [];
        this._order.push(mysql.escapeId(column) + ' DESC');
        return this;
    }

    proto.limits = function limits() {
        this._limits = Array.prototype.slice.call(arguments);
        return this;
    }

    proto.ignore = function ignore(value) {
        this._ignore = value;
        return this;
    }

    /* -- public methods returning a Promise -- */

    proto.update = function update() {
        if (this._data == null && this._query == null) {
            return Promise.reject(new Error("SQL: Missing data for an UPDATE query!"));
        }

        var query = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'UPDATE `' + this._table + '` SET ';
            query += buildData.call(this);
            if (conditions != null) query += " WHERE " + buildConditions.call(this);
            if (this._limit != null) query += " LIMIT " + buildLimit.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    }

    proto.upsert = function upsert() {
        this._upsert = true;
        return this.insert();
    }

    // called from within proto.insert if this._data is an array
    function insertMany() {
        if (this._data.length === 0) return Promise.resovle(false);

        var columns = Object.keys(this._data[0]);

        var columnsQuery = columns.map(mysql.escapeId).join(', '),
            query = 'INSERT ' + (this._ignore ? 'IGNORE ' : '') + 'INTO ' + mysql.escapeId(this._table) + ' (' + columnsQuery + ') VALUES ',
            rowFragments = [];

        query += this._data.map(function(row) {
            return '(' + columns.map(function(column) {
                return zeal.escape(row['column']);
            }).join(',') + ')';
        });

        return zeal.execute(query);
    }

    proto.insert = function insert() {
        var query = this._query;

        if (query == null) {
            if (Array.isArray(this._data)) return insertMany.call(this);

            query = 'INSERT ' + (this._ignore ? 'IGNORE ' : '') + 'INTO ' + mysql.escapeId(this._table) + ' SET ';

            var queryData = buildData.call(this);

            query += queryData;

            if (this._upsert) query += ' ON DUPLICATE KEY UPDATE ' + queryData;
        }

        return zeal.execute(query).then(function(result) {
            return result.insertId == null ? true : result.insertId;
        });
    }

    proto.erase = function erase() {
        var query = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'DELETE FROM ' + mysql.escapeId(this._table);
            if (conditions != null) query += ' WHERE ' + buildConditions.call(this);
            if (this._limit != null) query += ' LIMIT ' + buildLimit.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    }

    proto.truncate = function truncate() {
        var query = 'TRUNCATE TABLE ' + mysql.escapeId(this._table);

        return zeal.execute(query);
    }

    proto.one = function one() {
        return this.many().then(function(rows) {
            return rows.shift || null;
        });
    }

    proto.many = function many() {
        var query = this._query,
            conditions = this._conditions;

        if (query == null) {
            query = 'SELECT ' + buildSelect.call(this) + ' FROM ' + mysql.escapeId(this._table);

            if (conditions != null) query += ' WHERE ' + buildConditions.call(this);
            if (this._order != null) query += ' ORDER BY ' + this._order.join(', ');
            if (this._limit != null) query += ' LIMIT ' + buildLimit.call(this);

            conditions = null;
        }

        return zeal.execute(query, conditions);
    }

    proto.field = function field() {
        return this.one().then(function(row) {
            if (row == null) return null;
            var field = Object.keys(row)[0];
            return row[field] || null;
        });
    }

    proto.column = function column() {
        return this.many().then(function(rows) {
            if (rows.length === 0) return [];
            var field = Object.keys(rows[0])[0];
            return rows.map(function(row) {
                return row[field];
            });
        });
    }

    return Builder;
})();

var zeal = module.exports = (function(){
    var pool;

    return {
        configure: function(options) {
            if (pool) throw new Error('Can only call zeal.configure once!');

            pool = mysql.createPool(options);
        }

        escape: function(value) {
            if (Array.isArray(value)) return '(' + value.map(mysql.escape).join(',') + ')';
            return mysql.escape(value);
        }

        table: function(table) {
            return new Builder(table);
        }

        query: function(query, values) {
            return new Builder(null, query, values);
        }

        execute: function(query, values) {
            return new Promise(function(resolve, reject) {
                pool.query(query, values, function(err, result) {
                    if (err) return reject(err);

                    resolve(result);
                });
            });
        }

    }
})();