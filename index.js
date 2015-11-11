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
		// 	console.log('QUERY', e.query)
		// 	if (e.params)
		// 		console.log('PARAMS', e.params)
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
			arrays      = options.arrays || {},
			convertCase = Case[fieldCase || 'snake'],
			middleware  = _.merge(_.omit(options, 'table','scope','keys','model','transform','fieldCase','create','read','update','delete','middleware'), options.middleware || {})


		/**

		 Returns template parameter strings '${key} given key'

		 @param   {String} k - key to be used in parameter string
		 @returns {String}

		 */
		function templateParam(k) {
			var p = '${'+k+'}',	
				t = arrays[k]
			return t? p+'::'+t+'[]' : p
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
				let rawResult = yield postgres.result(sql,object)
				return rawResult.rowCount
			}

			let rows = yield postgres.anyCamelized(sql, object).map(function(object){
				return Joi.validate(object, model, {stripUnknown:true}).value	
			})
			
			if (single && Array.isArray(rows))
				return rows.length? rows[0] : null
			
			return rows
		}


		/**

		 Returns an object in place of an Error.

		 */
		function errorObject(message, object) {
			if (object instanceof Error)
				return {error$:{message, error:object}}
			return object
		}


		function* noopMiddleware(batch, next) {
			return yield next(batch)
		} 

		function handlerWrapper(options) {

			options = _.defaults(options, {
				batch: false,
				key: false,
				failureMessage: 'failed',
				middleware: noopMiddleware,
				forceMany:false
			})

			var batchQuery = options.batch,
				key        = options.key,
				middleware = options.middleware || noopMiddleware,
				handler    = options.handler,
				forceMany  = options.forceMany

			if (!isGeneratorFunction(middleware))
				middleware = noopMiddleware


			return function*() {

				let req      = _.clone(this.req || {}),
					scopeVal = scope && this.req[scope],
					batch    = req.batch || key && req[key] || [req],
					many     = forceMany || req.batch || key && Array.isArray(req[key])

				if (!Array.isArray(batch))
					batch = [batch]

				if (scope && !req[scope])
					throw new Error('missingScope ' + scope)

				if (key && !req[key] && !req.batch)
					throw new Error('missingKey')


				var next

				if (batchQuery) {
					// Do all objects in on query
					next = function*(batch) {
						var params = {}
						params[key] = batch
						if (scope) params[scope] = scopeVal
						return yield handler(params)
					}
				}
				else {
					// Loop function on each object in batch (parallel)
					next = function*(batch) {
						return yield batch.map(function(o){
							return co(function*(){
								try { 
									if (scope) o[scope] = scopeVal
									return yield handler(o) 
								}
								catch(err) { 
									return err 
								}
							})
						})
					}
				}

				var result = yield middleware(batch, next)

				if (Array.isArray(result)) {
					if (!many) {
						result = result[0]
						if (result instanceof Error)
							throw result
					} else {
						result = result.map(errorObject.bind(this, options.failureMessage || 'failed'))
					}
				}
				return result
			}
		}

		function objectValidationModel() {

			var base, one, many, oneOrMany, keys

			one  = _.clone(model)
			many = {batch: Joi.array().items(model)}

			// Not required because handled in handleWrapper 
			if (scope) {
				many[scope] = model[scope]
			}

			one  = Joi.object().keys(one)
			many = Joi.object().keys(many)

			return Joi.alternatives().try([one, many])
		}

		function keyedValidationModel(key) {

			var base, one, many

			base = model[key]
			
			one = {}
			one[key] = base

			many = {}
			many[key]  = Joi.array().items(base),
			many.batch = many[key]

			// Not required because handled in handleWrapper
			if (scope)
				one[scope] = many[scope] = model[scope]

			one =  Joi.object().keys(one).options({stripUnknown:true})
			many = Joi.object().keys(many).xor(key, 'batch').options({stripUnknown:true})
			
			var toto = Joi.alternatives().try([one, many])

			return toto
		}


		// ------------------------------------------------------------------------
		// Create
		// ------------------------------------------------------------------------

		if (options.create) {

			try {

				let action = _.merge(middleware || {}, {
					name: path + '.create',
					descriptions: 'Creates one or more records from the provided values.',
					handler: handlerWrapper({
						failureMessage: 'insertFailed', 
						middleware: options.create, 
						handler: function*(o) {
							return yield SQLRunQuery(SQLInsert(o), o, true)
						}
					})
				})

				action[validate] = {request: objectValidationModel()}
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

				// Create a fetch.by<Key> for each key
				_.each(keys, function(type, key){					

					let action = _.merge(middleware || {}, {
						name: path + '.fetch.' + Case.camel('by-'+key),
						description: 'Fetch one or more ' + path + ' records by ' + key + '.',
						handler: handlerWrapper({
							batch:true, 
							key, 
							failureMessage: 'readFailed', 
							middleware: options.read, 
							handler: function*(batch) {
								return yield SQLRunQuery(SQLSelect(scope, key, type), batch, false)
							}
						})
					})

					action[validate] = {request: keyedValidationModel(key)}
					service.action(action)
				})


				// fetch
				let action = _.merge(middleware || {}, {
					name: path + '.fetch',
					description: 'Fetch all records in the collection',
					handler: handlerWrapper({
						batch: true, 
						failureMessage: 'readFailed', 
						middleware: options.read, 
						forceMany:true,
						handler: function*(batch) {
							return yield SQLRunQuery(SQLSelect(scope), batch, false)
						}
					}) 
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
				handler: handlerWrapper({
					failureMessage: 'updateFailed', 
					middleware: options.update, 
					handler: function*(o) {
						return yield SQLRunQuery(SQLUpdate(o), o, true)
					}
				})
			})

			action[validate] = { request: objectValidationModel() }
			service.action(action)
		}


		// ------------------------------------------------------------------------
		// Delete
		// ------------------------------------------------------------------------

		if (options.delete) {

			try {

				// Create a delete.by<Key> for each key
				_.each(keys, function(type, key){					

					let action = _.merge(middleware || {}, {
						name: path + '.delete.' + Case.camel('by-'+key),
						description: 'Deletes one or more ' + path + ' records by ' + key + '.',
						handler: handlerWrapper({
							batch: true, 
							key, 
							failureMessage: 'deleteFailed', 
							middleware: options.delete, 
							handler: function*(o) {
								return yield SQLRunQuery(SQLDelete(scope, key, type), o, false)
							}
						})
					})

					action[validate] = {request:keyedValidationModel(key)}
					service.action(action)
				})


				// delete.all only if scope
				if (scope) {
					let action = _.merge(middleware || {}, {
						name: path + '.delete.all',
						description: 'Deletes all records in the collection',
						handler: handlerWrapper({
							failureMessage: 'deleteFailed', 
							middleware: options.delete, 
							handler: function*(o) {
								return yield SQLRunQuery(SQLDelete(scope), o, false)
							}
						}) 
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



		// ------------------------------------------------------------------------
		// Merge
		// ------------------------------------------------------------------------

		if (options.merge) {

			try {
				let action = _.merge(middleware || {}, {
					name: path + '.merge',
					descriptions: 'Merges (updates or inserts as needed) a record changing or setting only fields that have been provided.',
					handler: handlerWrapper({
						failureMessage: 'mergeFailed', 
						middleware: options.merge, 
						handler: function*(o) {
							var record = null
							while (!record) {
								record = yield SQLRunQuery(SQLUpdate(o), o, true)
								if (record == null) {
									try {
										record = yield SQLRunQuery(SQLInsert(o), o, true)
									}
									catch(err) {
										console.error(err)
										if (!/duplicate key/.test(err.mesage))
											throw err
									}
								}
							}
							return  record
						}
					})
				})

				action[validate] = { request: objectValidationModel() }
				service.action(action)
			}
			catch(err) {
				console.error(err.stack)
				throw err
			}
		}		
	}
}

// From co
function isGeneratorFunction(obj) {
  var constructor = obj.constructor;
  if (!constructor) return false;
  if ('GeneratorFunction' === constructor.name || 'GeneratorFunction' === constructor.displayName) return true;
  return isGenerator(constructor.prototype);
}

function isGenerator(obj) {
  return 'function' == typeof obj.next && 'function' == typeof obj.throw;
}
