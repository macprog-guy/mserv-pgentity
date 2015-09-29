'use strict'

var Promise = require('bluebird'),
	debug   = require('debug')('mserv-pgentity'),
	recase  = require('recase-keys'),
	Case    = require('case'),
	Joi     = require('joi'),
	co      = require('co'),
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
		extend: pgext
		// query: function(e) {
		// 	console.log([e.query, e.params])
		// }
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

		let table       = options.table,
			keys        = options.keys,
			model       = options.model,
			scope       = options.scope,
			fieldCase   = options.fieldCase,
			convertCase = Case[fieldCase || 'snake'],
			middleware  = _.merge(_.omit(options, 'table','scope','keys','model','transform','fieldCase','create','read','update','delete','middleware'), options.middleware || {})

		/**

		 Returns a Joi model that accepts the input model or an object with an 'objects' key 
		 whose value is an array of input models.

		 @param {Joi} model - input model is either a Joi model or a plain object whose keys are Joi models.
		 @returns {Joi} 

		 @api private

		 */

		function singleOrArrayOfModels(model) {

			let compositeModel = model

			// Convert the model to a Joi object
			if (!compositeModel.isJoi && typeof compositeModel === 'object')
				compositeModel = Joi.object().keys(compositeModel)

			// Accept one model object or an array of model batch
			// For the array, the request parameter is "batch".
			let keys = {batch: Joi.array().single(true).items(compositeModel).required().options({stripUnknown:true})}
			if (scope)
				keys[scope] = model[scope].required()

			return Joi.alternatives().try([
				Joi.object().keys(keys),
				model
			])
		}

		/**

		 Returns template parameter strings '${key} given key'

		 @param   {String} k - key to be used in parameter string
		 @returns {String}

		 */
		function templateParam(k) {
			return '${'+k+'}' 
		}

		/**

		 Returns 'key = ${key}' given key.

		 @param   {String} k - key to be used in the assignment
		 @returns {String}

		 */
		function templateAssign(k) {
			return convertCase(k) + '=' + templateParam(k)
		}


		/**

		 Return an insert statement using only the objects values.

		 @return {String} postgres insert statement.
		 @api private

		 */
		function SQLInsert(object) {

			let keys    = _.keys(object),
				columns = _.map(keys, convertCase),
				values  = _.map(keys, templateParam)

			return `insert into ${table} (${columns}) values (${values}) returning *`
		}


		/**

		 Return a select statement using the specified scope and key fields.

		 @return {String} postgres select statement.
		 @api private

		 */
		function SQLSelect(scope, key, keyType, values) {

			let conds = []

			if (scope) conds.push(templateAssign(scope))
			if (key)   conds.push(convertCase(key) +' = any(${'+key+'}::'+keyType+'[])')

			return (conds.length)?
				`select * from ${table} where ${conds.join(' and ')}` :
				`select * from ${table}`
		}


		/**

		 Return an update statement using the object fields and values only.

		 @return {String} postgres select statement.
		 @api private

		 */
		function SQLUpdate(object) {

			for (var key in keys) {
				if (object[key]) {

					let assigns = _(object).omit(key,scope).keys().map(templateAssign).values(),
						conds   = [templateAssign(key)]
						
					if (scope)
						conds.unshift(templateAssign(scope))

					return `update ${table} set ${assigns} where ${conds.join(' and ')} returning *`
				}
			}
			throw new Error('missingKey')
		}



		/**

		 Return a delete statement using the one of the keys and the scope

		 @return {String} postgres select statement.
		 @api private

		 */
		function SQLDelete(scope, key, keyType) {

			let conds = []

			if (key)
				conds.push(convertCase(key) +' = any(${'+key+'}::'+keyType+'[])')

			if (scope)
				conds.unshift(templateAssign(scope))

			return `delete from ${table} where ${conds.join(' and ')}`
		}


		/**

		 Processes a query and converts the returned values using the Joi model.

		 @return {String} postgres select statement.
		 @api private

		 */
		function* SQLRunQuery(sql, object, single) {
			
			debug({sql, object, single})

			if (sql.slice(0,6).toLowerCase() === 'delete') {
				let rawResult = yield postgres.query(sql,object,'result')
				return rawResult.rowCount
			}

			let rows = yield postgres.anyCamelized(sql, object).map(function(object){
				return Joi.validate(object, model, {stripUnknown:true}).value	
			})
			
			return single && rows.length >= 1? rows[0] : rows
		}


		/**

		 Returns an object in place of an Error.

		 */
		function errorObject(message, object) {
			if (object instanceof Error)
				return {error$:{message, error:object}}
			return object
		}



		// ------------------------------------------------------------------------
		// Create
		// ------------------------------------------------------------------------

		if (options.create) {

			try {
				let action = _.merge(middleware || {}, {
					name: path + '.create',
					descriptions: 'Creates one or more records from the provided values.',
					handler: function*() {

						let self     = this,
							req      = self.req || {},						
							scopeVal = scope && self.req[scope],
						    batch    = req.batch || [req]

						// Check that we have the scope key
						if (scope && !this.req[scope])
							throw new Error('missingScope ' + scope)

						let result = yield batch.map(function(o) {
							return co(function*(){
								try {
									if (scope) {
										o = _.clone(o)
										o[scope] = scopeVal
									}
									return yield SQLRunQuery(SQLInsert(o), o, true)
								}
								catch(error) {
									return error
								}
							})
						})

						if (req.batch)
							return result.map(errorObject.bind(this, 'insertFailed'))

						// Take the first result if we have one
						result = result.length? result[0] : null

						// Throw if it's an error
						if (result instanceof Error)
							throw result

						return  result
					}
				})

				action[validate] = {request: singleOrArrayOfModels(model)}
				service.action(action)
			}
			catch(err) {
				console.error(err)
				throw err
			}
		}


		// ------------------------------------------------------------------------
		// Read
		// ------------------------------------------------------------------------

		if (options.read) {

			try {

				// Create a fetch.byKey for each key
				_.each(keys, function(type, key){					

					let action = _.merge(middleware || {}, {
						name: path + '.fetch.' + Case.camel('by-'+key),
						description: 'Fetch one or more ' + path + ' records by ' + key + '.',
						handler: function*() {

							let req = this.req || {},
								keys   = req[key],
								single = !Array.isArray(keys),
								object = {}

							if (scope && !req[scope])
								throw new Error('missingScope ' + scope)

							object[key] = single? [keys] : keys
							if (scope)
								object[scope] = req[scope]

							let result = yield SQLRunQuery(SQLSelect(scope, key, type), object, single)

							if (result instanceof Error)
								throw result

							return result
						}
					})

					let fetchModel = {}

					// Make the key take ether a value or an array of values
					fetchModel[key] = Joi.alternatives().try([
						Joi.array().items( model[key] ).required(),
						model[key].required()
					])

					// Make the scope mandatory if specified
					if (scope) fetchModel[scope] = model[scope].required()

					action[validate] = {request:fetchModel}
					service.action(action)
				})


				// fetch.all
				let action = _.merge(middleware || {}, {
					name: path + '.fetch.all',
					description: 'Fetch all records in the collection',
					handler: function*(){

						let req    = this.req || {},
							object = {}

						if (scope && !req[scope])
							throw new Error('missingScope ' + scope)

						if (scope)
							object[scope] = req[scope]

						return yield SQLRunQuery(SQLSelect(scope), object, false)
					} 
				})

				if (scope) {
					let fetchModel = {}
					fetchModel[scope] = model[scope].required()
					action[validate]  = {request:fetchModel}
				}

				service.action(action)
			}
			catch(err) {
				console.error(err.stack)
				throw err
			}
		}




		// ------------------------------------------------------------------------
		// Update
		// ------------------------------------------------------------------------

		if (options.update) {

			let action = _.merge(middleware || {}, {
				name: path + '.update',
				descriptions: 'Updates a record changing only fields that have been provided.',
				handler: function*() {

					let self     = this,
						req      = self.req || {},
						scopeVal = scope && req[scope],
					    batch  = req.batch || [req]

					if (scope && !req[scope])
						throw new Error('missingScope ' + scope)

					let result = yield batch.map(function(o) {
						return co(function*(){
							try {
								if (scope) {
									o = _.clone(o)
									o[scope] = scopeVal
								}
								return yield SQLRunQuery(SQLUpdate(o), o, true)
							}
							catch(error) {
								return error
							}
						})
					})

					if (req.batch)
						return result.map(errorObject.bind(this, 'updateFailed'))

					// Take the first result if we have one
					result = result.length? result[0] : null

					// Throw if it's an error
					if (result instanceof Error)
						throw result

					return  result
				}
			})

			action[validate] = { request: singleOrArrayOfModels(model) }
			service.action(action)
		}


		// ------------------------------------------------------------------------
		// Delete
		// ------------------------------------------------------------------------

		if (options.delete) {

			try {

				// Create a fetch.byKey for each key
				_.each(keys, function(type, key){					

					let action = _.merge(middleware || {}, {
						name: path + '.delete.' + Case.camel('by-'+key),
						description: 'Deletes one or more ' + path + ' records by ' + key + '.',
						handler: function*() {

							let req = this.req || {},
								keys   = req[key] || [],
								single = !Array.isArray(keys),
								object = {}

							if (scope && !req[scope])
								throw new Error('missingScope ' + scope)

							object[key] = single? [keys] : keys
							if (scope)
								object[scope] = req[scope]

							if (!object[key].length)
								throw new Error('missingKey')

							let result = yield SQLRunQuery(SQLDelete(scope, key, type), object, single)

							if (result instanceof Error)
								throw result

							return result
						}
					})

					let deleteModel = {}

					// Make the key take ether a value or an array of values
					deleteModel[key] = Joi.alternatives().try([
						Joi.array().items( model[key] ).required(),
						model[key].required()
					])

					// Make the scope mandatory if specified
					if (scope) deleteModel[scope] = model[scope].required()

					action[validate] = {request:deleteModel}
					service.action(action)
				})


				// delete.all only if scope
				if (scope) {
					let action = _.merge(middleware || {}, {
						name: path + '.delete.all',
						description: 'Deletes all records in the collection',
						handler: function*(){

							let req    = this.req || {},
								object = {}

							if (scope && !req[scope])
								throw new Error('missingScope ' + scope)

							if (scope)
								object[scope] = req[scope]

							return yield SQLRunQuery(SQLDelete(scope), object, false)
						} 
					})

					let deleteModel = {}
					deleteModel[scope] = model[scope].required()
					action[validate]  = {request:deleteModel}

					service.action(action)
				}
			}
			catch(err) {
				console.error(err.stack)
				throw err
			}
		}	
	}
}


