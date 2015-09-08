'use strict'

var Promise = require('bluebird'),
	recase  = require('recase-keys'),
	Case    = require('case'),
	Joi     = require('joi'),
	PGLib   = require('pg-promise'),
	pgext   = require('./utils/pgext'),
	_       = require('lodash')


module.exports = function(service, options) {

	options = _.defaults(options, {
		context: 'context',
		validate: 'validate',
		postgres:'postgres://localhost'
	})

	let PG = PGLib({
		promiseLib: Promise,
		extend: pgext,
		// query: function(e) {
		// 	console.log([e.query, e.params])
		// },
		// error: function(err, e) {
		// 	console.log(err)
		// 	if (e) {
		// 		console.log(e.query)
		// 		if (e.params)
		// 			console.log(e.params)
		// 	}
		// }
	})

	var postgres = PG(options.postgres),
	 	validate = options.validate

	// If the context middleware is registered then add postgres to it.
	if (service.ext[options.context]) {
		service.ext[options.context](function(){
			this.ctx.postgres = postgres
		})
	}

	// Add an extension making postgres accessible globally
	if (!service.ext.postgres) {
		service.extend('postgres', function(){
			return function() { return postgres }
		})
	}


	return function(path, options) {

		if (!postgres)
			throw new Error('Postgres options is not defined')

		let table = options.table,
			keys  = options.keys,
			model = options.model,
			transform   = options.transform   || function(x){ return x },
			convertCase = options.convertCase || false,
			middleware  = _.merge(_.omit(options, 'table','keys','model','transform','convertCase','create','read','update','delete','middleware'), options.middleware || {})

		transform = transform.bind(this)


		// ------------------------------------------------------------------------
		// Create
		// ------------------------------------------------------------------------

		if (options.create) {

			let action = _.merge(middleware || {}, {
				name: path + '.create',
				descriptions: 'Creates a record from the provided values.',
				handler: function*() {
					let insert = SQLInsert(table, this.req, convertCase),
						record = yield pgCleansedQuery(insert, model, this.req, convertCase, true)

					return transform(record)
				}
			})

			action[validate] = {request: model}

			service.action(action)
		}


		// ------------------------------------------------------------------------
		// Read
		// ------------------------------------------------------------------------

		if (options.read) {

			_.each(keys, function(type, key){

				let action = _.merge(middleware || {}, {
					name: path + '.fetch.' + Case.camel('by-'+key),
					description: 'Fetch one or more ' + path + ' records by their ' + key + '.',
					handler: function*() {

						let id     = this.req[key],
							values = Array.isArray(id)? id : [id],
							select = SQLSelect(table, key, type, convertCase),
							rows   = yield pgCleansedQuery(select, model, [values], convertCase, !Array.isArray(id))

			    		return transform(rows)
					}
				})

				let fetchByKeyModel  = {}
				fetchByKeyModel[key] = Joi.array().single(true).items( model[key] ).required().options({stripUnknown:false})

				action[validate] = {request: fetchByKeyModel}
				service.action(action)
			})
		}


		// ------------------------------------------------------------------------
		// Update
		// ------------------------------------------------------------------------

		if (options.update) {

			let updateModel = Joi.object().keys( model ).or(Object.keys(keys))

			let action = _.merge(middleware || {}, {
				name: path + '.update',
				descriptions: 'Updates a record changing only fields that have been provided.',
				handler: function*() {

					let update = SQLUpdate(table, keys, this.req, convertCase),
						record = yield pgCleansedQuery(update, model, this.req, convertCase, true)

					return transform(record)
				}
			})

			action[validate] = { request: updateModel }
			service.action(action)
		}


		// ------------------------------------------------------------------------
		// Delete
		// ------------------------------------------------------------------------

		if (options.delete) {

			let deleteModel = Joi.object().keys( model ).or(Object.keys(keys))

			let action = _.merge(middleware || {}, {
				name: path + '.delete',
				descriptions: 'Deletes a record based on a key.',			
				handler: function*() {

					let del  = SQLDelete(table, keys, this.req, convertCase),
						rows = pgCleansedQuery(del, model, this.req, convertCase, true)

					return transform(rows)
				}
			})

			action[validate] = deleteModel
			service.action(action)
		}	
	}










	// ------------------------------------------------------------------------
	// Query Execution
	// ------------------------------------------------------------------------

	function* pgCleansedQuery(sql, model, params, convertCase, single) {
		
		if (!sql) return null

		let rows = yield postgres.anyCamelized(sql, params).map(function(object){
			return Joi.validate(object, model, {stripUnknown:true}).value	
		})
		
		if (convertCase)
			rows = rows.map(function(object){ return recase.toSnake(object)	})

		return single? (rows.length >= 1? rows[0] : undefined) : rows
	}


	// ------------------------------------------------------------------------
	// Statement Builders
	// ------------------------------------------------------------------------

	function SQLInsert(table, object, convertCase) {

		let columns = [],
			values  = []

		_.each(object, function(value, prop){
			columns.push(convertCase? Case.snake(prop) : prop)
			values.push('${'+prop+'}')
		})

		return `insert into ${table} (${columns}) values (${values}) returning ${table}.*`
	}


	function SQLSelect(table, key, type, convertCase) {
		let keyCol = convertCase? Case.snake(key) : key
		return `select * from ${table} where ${key} = any(\$1::${type}[])`
	}


	function SQLUpdate(table, keys, object, convertCase) {

		for (let key in keys) {
			if (object[key]) {

				let assigns = _(object).omit(key).keys().map(function(prop){
					let column = convertCase? Case.snake(prop) : prop
					return column + '=${'+prop+'}'
				}).values()

				let keyCol = convertCase? Case.snake(key) : key,
				    keyCond = '${'+key+'}'

				return `update ${table} set ${assigns} where ${keyCol}=${keyCond} returning ${table}.*`
			}
		}
		return null
	}	


	function SQLDelete(table, keys, object, convertCase) {
		for (let key in keys) {
			if (object[key]) {
				let keyCol  = convertCase? Case.snake(key) : key,
					keyCond = '${'+key+'}'
				return `delete from ${table} where ${keyCol}=${keyCond}`
			}
		}
		return null
	}
}
