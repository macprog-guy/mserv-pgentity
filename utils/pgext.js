/**

PGExt

Handy functions when calling querying postgres with pg-promise.

*/

'use strict'

var recase = require('recase-keys')

function camelizeRows(rows, klass) {
	rows = rows.map(recase.toCamel)
	if (klass)
		rows = rows.map(function(row){ return new klass(row) }) 
	return rows
}

module.exports = function(db) {
	
	db.funcCamelized = function(name, params, klass) {
		return db.func(name, params)
			.then(function(rows){ 
				return camelizeRows(rows, klass) 
			})
	}

	db.funcCamelized1 = function(name, params, klass) {
		return db.funcCamelized(name, params, klass)
			.then(function(rows){
				return (rows.length >= 1)? rows[0] : undefined
			})
	}

	db.anyCamelized = function(query, values, klass) {
		return db.any(query, values)
			.then(function(rows){
				return camelizeRows(rows, klass)
			})
	}

	db.anyCamelized1 = function(query, values, klass) {
		return db.anyCamelized(query, values, klass)
			.then(function(rows){ 
				return (rows.length >= 1)? rows[0] : undefined
			})
	}

	db.count = function(table, conds) {
		let query = conds?
			`select count(*) from ${table} where ${conds.join(' and ')}` :
			`select count(*) from ${table}`
		
		return db.one(query).then(function(row){
			return parseInt(row.count)
		})
	}
}