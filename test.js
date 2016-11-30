'use strict'

var mserv    = require('mserv'),
	entity   = require('.'),
	validate = require('mserv-validate'),
	Joi      = require('joi'),
	chai     = require('chai'),	
	should   = chai.should(),
	co       = require('co'),
	_        = require('lodash')


var TodoModel = {	
	id:      Joi.string().guid(),
	name:    Joi.string().allow(null),
	done:    Joi.boolean(),
	tags:    Joi.array().items(Joi.string())
}

var ScopedTodoModel = {	
	ownerId: Joi.number(),
	id:   Joi.string().guid(),
	name: Joi.string().allow(null),
	done: Joi.boolean()
}


// Helper function makes tests less verbose
function wrappedTest(generatorFunc) {
	return function(done) {
		try {
			co(generatorFunc)
			.then(
				function()   { done()    },
				function(err){ done(err) }
			)
		}
		catch(err) {
			done(err)
		}
	}
}

	




// ------------------------------------------------------------------------
//
// Without mserv-validate
//
// ------------------------------------------------------------------------

describe('mserv-pgentity without mserv-validate', function(){

	let service  = mserv({amqp:false}).extend('entity',entity),
		postgres = service.ext.postgres(),
		hooks    = []

	service.ext.entity('todo', {
		table:'todos',
		keys: {id:'uuid'},
		arrays: {tags:'text'},
		model: TodoModel,
		create: function*(batch, next){
			var records = yield next(batch)
			hooks.push(records.length)
			return records
		},
		read: true,
		update: true,
		delete: true,
		merge: true
	})

	service.ext.entity('scopedTodo', {
		table:'scopedTodos',
		scope:'ownerId',
		keys: {id:'uuid'},
		model: ScopedTodoModel,
		create: true,
		read: true,
		update: true,
		delete: true,
		merge: true
	})


	before(function(done){
		postgres.none('create extension if not exists pgcrypto').then(function(){
			postgres.none('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text not null, done boolean default false, tags text[] default null, created_at timestamp default current_timestamp)').then(function(){
				postgres.none('create table if not exists scopedTodos (owner_id int, id  uuid not null primary key default gen_random_uuid(), name text not null, done boolean default false, created_at timestamp default current_timestamp)').nodeify(done)
			})
		})		
	})

	after(function(done){
		postgres.none('drop table todos; drop table scopedTodos').nodeify(done)
	})

	beforeEach(function(done){
		hooks = [],
		postgres.none('truncate table todos; truncate table scopedTodos').nodeify(done)
	})


	// ------------------------------------------------------------------------
	// Create
	// ------------------------------------------------------------------------

	it('create should return a record', wrappedTest(function*(){

		let rec1 = {name: 'item #1', done:false},
			rec2 = yield service.invoke('todo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)

		// Our model does not have the createdAt column even though the DB does.
		rec2.should.not.have.property.createdAt

		// Ensure that our create "middleware" has been called.
		hooks.should.eql([1])
	}))

	it('create should throw constraint violation', wrappedTest(function*(){

		try {
			// mserv-validate not installed
			yield service.invoke('todo.create', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	it('create should return multiple records', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false},{name: 'item #2', done:true}],
			recs2 = yield service.invoke('todo.create', {batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)
		recs2[1].name.should.equal(recs1[1].name)
		recs2[1].done.should.equal(recs1[1].done)

		// Ensure that our create "middleware" has been called.
		hooks.should.eql([2])
	}))



	it('create should return one record and one error', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false}, {done:true}],
			recs2 = yield service.invoke('todo.create', {batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)
		recs2[1].should.have.property.error$
	}))

	it('create should return a record with empty tags array', wrappedTest(function*(){

		let rec1 = {name: 'item #1', done:false, tags:[]},
			rec2 = yield service.invoke('todo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)
		rec2.tags.should.eql(rec1.tags)
	}))

	it('create should return a record with non-empty tags array', wrappedTest(function*(){

		let rec1 = {name: 'item #1', done:false, tags:['tag1','tag2']},
			rec2 = yield service.invoke('todo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)
		rec2.tags.should.eql(rec1.tags)
	}))


	// ------------------------------------------------------------------------
	// Create Scoped
	// ------------------------------------------------------------------------

	it('scoped create should return a record', wrappedTest(function*(){

		let rec1 = {ownerId:1, name: 'item #1', done:false},
			rec2 = yield service.invoke('scopedTodo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.ownerId.should.equal(rec1.ownerId)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)

		// Our model does not have the createdAt column even though the DB does.
		rec2.should.not.have.property.createdAt
	}))

	it('scoped create should throw missing scope', wrappedTest(function*(){

		try {
			// mserv-validate not installed
			yield service.invoke('scopedTodo.create', {name:'item #2', done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	it('scoped create should return multiple records', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false},{name: 'item #2', done:true}],
			recs2 = yield service.invoke('scopedTodo.create', {ownerId:1, batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].ownerId.should.equal(1)
		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)

		recs2[1].ownerId.should.equal(1)
		recs2[1].name.should.equal(recs1[1].name)
		recs2[1].done.should.equal(recs1[1].done)
	}))




	// ------------------------------------------------------------------------
	// Read
	// ------------------------------------------------------------------------

	it('fetch.byId should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})

		rec2.should.eql(rec1)
	}))

	it('fetch.byId should return many records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:true}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false}),
			recs = yield service.invoke('todo.fetch.byId', {id:[rec1.id, rec2.id, rec3.id]})

		should.exist(recs)
		recs.length.should.equal(3)
		
		recs[0].name.should.equal(rec1.name)
		recs[1].name.should.equal(rec2.name)
		recs[2].name.should.equal(rec3.name)

		recs[0].done.should.equal(rec1.done)
		recs[1].done.should.equal(rec2.done)
		recs[2].done.should.equal(rec3.done)
	}))


	it('fetch.byId should throw invalid input syntax for uuid', wrappedTest(function*(){
		try {
			let rec1 = yield service.invoke('todo.fetch.byId', {id:'not-a-uuid'})
			throw new Error('Invoke did not throw')
		}
		catch(err){
			if (err.message === 'Invoke did not throw')
				throw err
			_.pick(err,'name','message').should.eql({
				name: 'error',
				message: 'invalid input syntax for uuid: "not-a-uuid"'
			})
		}
	}))


	it('fetch should return many records', wrappedTest(function*(){

		yield postgres.none(`insert into todos (name, done) values ('item1',false),('item2',true),('item3',false)`)

		let recs = yield service.invoke('todo.fetch')

		should.exist(recs)
		recs.length.should.equal(3)		
	}))



	// ------------------------------------------------------------------------
	// Read Scoped
	// ------------------------------------------------------------------------

	it('scoped fetchById should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.fetch.byId', {ownerId:1, id:rec1.id})

		rec2.should.eql(rec1)
	}))

	it('scoped fetchById should return many records', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #2', done:true}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false}),
			recs = yield service.invoke('scopedTodo.fetch.byId', {ownerId:2, id:[rec1.id, rec2.id, rec3.id]})

		should.exist(recs)
		recs.length.should.equal(3)
		
		recs[0].name.should.equal(rec1.name)
		recs[1].name.should.equal(rec2.name)
		recs[2].name.should.equal(rec3.name)

		recs[0].done.should.equal(rec1.done)
		recs[1].done.should.equal(rec2.done)
		recs[2].done.should.equal(rec3.done)
	}))


	it('scoped fetchById should throw missingScope', wrappedTest(function*(){
		try {
			yield service.invoke('scopedTodo.fetch.byId', {id:'12345678-1234-1234-1234-123456789012'})
			throw new Error('Invoke did not throw')
		}
		catch(err){
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))

	it('scoped fetch should return many records', wrappedTest(function*(){

		yield postgres.none(`insert into scopedTodos (owner_id, name, done) values (1,'item1',false),(1,'item2',true),(2,'item3',false),(2,'item4',false)`)

		let recs = yield service.invoke('scopedTodo.fetch', {ownerId:1})

		should.exist(recs)
		recs.length.should.equal(2)
		recs[0].ownerId.should.equal(1)
		recs[1].ownerId.should.equal(1)
	}))

	it('scoped fetch throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.fetch')
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	// ------------------------------------------------------------------------
	// Update
	// ------------------------------------------------------------------------

	it('update should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

		rec2.id.should.equal(rec1.id)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(true)
		rec2.should.not.have.property.createdAt
	}))


	it('update should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('todo.update', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('update should return multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		rec1.done = true,
		rec2.done = true,
		rec3.done = true

		let recs = yield service.invoke('todo.update', {batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[1].id.should.equal(rec2.id)
		recs[2].id.should.equal(rec3.id)

		recs[0].done.should.equal(true)
		recs[1].done.should.equal(true)
		recs[2].done.should.equal(true)
	}))

	it('update should return some records and some errors', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		rec1.done = true
		rec2.done = true
		rec2.name = null
		rec3.done = true	

		let recs = yield service.invoke('todo.update', {batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[2].id.should.equal(rec3.id)

		recs[0].done.should.equal(true)
		recs[2].done.should.equal(true)

		recs[1].should.have.property.error$
	}))


	// ------------------------------------------------------------------------
	// Update Scoped
	// ------------------------------------------------------------------------

	it('scoped update should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.update', {ownerId:1, id:rec1.id, done:true})

		rec2.id.should.equal(rec1.id)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(true)
		rec2.should.not.have.property.createdAt
	}))

	it('scoped update should throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.update', {id:'12345678-1234-1234-1234-123456789012', done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingScope ownerId')
				throw err
		}
	}))

	it('scoped update should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.update', {ownerId:3, done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('scoped update should return multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		rec1.done = true,
		rec2.done = true,
		rec3.done = true

		delete rec1.ownerId
		delete rec2.ownerId

		let recs = yield service.invoke('scopedTodo.update', {ownerId:1, batch:[rec1, rec2]})

		should.exist(recs)
		recs.length.should.equal(2)

		recs[0].ownerId.should.equal(1)
		recs[0].id.should.equal(rec1.id)
		recs[0].done.should.equal(true)

		recs[1].ownerId.should.equal(1)
		recs[1].id.should.equal(rec2.id)
		recs[1].done.should.equal(true)
	}))


	it('scoped update should return some records and some errors', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		rec1.done = true
		rec2.done = true
		rec2.name = null
		rec3.done = true	

		delete rec1.ownerId
		delete rec2.ownerId

		let recs = yield service.invoke('scopedTodo.update', {ownerId:1, batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[0].done.should.equal(true)

		recs[1].should.have.property.error$
	}))	



	// ------------------------------------------------------------------------
	// Delete
	// ------------------------------------------------------------------------


	it('delete.byId should delete single records', wrappedTest(function*(){

		let rec1  = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			count = yield service.invoke('todo.delete.byId', {id:rec1.id})

		count.should.equal(1)
	}))


	it('delete.byId should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('todo.delete.byId', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('delete.byId should delete multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		let count = yield service.invoke('todo.delete.byId', {id:[rec1.id, rec2.id, rec3.id]})

		count.should.equal(3)
	}))

	it('delete.byId should ignore unknown keys', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false})

		let count = yield service.invoke('todo.delete.byId', {id:[
			rec1.id, 
			'12345678-1234-1234-1234-123456789012',
			rec2.id, 
			'12345678-1234-4567-8901-123456789012'
		]})

		count.should.equal(2)
	}))

	


	// ------------------------------------------------------------------------
	// Delete Scoped
	// ------------------------------------------------------------------------


	it('scoped delete.byId should delete single records', wrappedTest(function*(){

		let rec1  = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:rec1.id})

		count.should.equal(1)
	}))

	it('scoped delete.byId should throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.delete.byId', {id:'12345678-1234-1234-1234-123456789012'})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingScope ownerId')
				throw err
		}
	}))

	it('scoped delete.byId should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.delete.byId', {ownerId:1})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('scoped delete.byId should only delete records within scope', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false}),
			rec4 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false}),
			rec5 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		let count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:[rec1.id, rec2.id, rec3.id, rec4.id, rec5.id]})
		count.should.equal(3)

		let recs = yield service.invoke('scopedTodo.fetch', {ownerId:2})
		recs.length.should.equal(2)
	}))

	it('scoped delete.byId should ignore unknown ids', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false})

		let count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:[
			rec1.id, 
			'12345678-1234-1234-1234-123456789012',
			rec2.id, 
			'12345678-1234-4567-8901-123456789012',
		]})

		count.should.equal(2)
	}))


	it('scoped delete.all should only delete records within scope', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false}),
			rec4 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #1', done:false}),
			rec5 = yield service.invoke('scopedTodo.create', {ownerId:3, name: 'item #2', done:false})

		let count = yield service.invoke('scopedTodo.delete.all', {ownerId:1})

		count.should.equal(3)
	}))



	// ------------------------------------------------------------------------
	// Merge 
	// ------------------------------------------------------------------------

	it('merge should insert a new record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.merge', {id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		_.pick(rec1, 'id','name','done').should.eql({id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
	}))


	it('merge should update the existing record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.merge', {id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.done = true
		let rec2 = yield service.invoke('todo.merge', rec1),
			rec3 = yield service.invoke('todo.fetch.byId', {id:'12345678-1234-1234-1234-123456789012'})

		rec2.should.eql(rec3)
		rec3.done.should.be.true
	}))

	// ------------------------------------------------------------------------
	// Merge Scoped
	// ------------------------------------------------------------------------

	it('scoped merge should insert a new record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.merge', {ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.should.eql({ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
	}))


	it('scoped merge should update the existing record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.merge', {ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.done = true
		let rec2 = yield service.invoke('scopedTodo.merge', rec1),
			rec3 = yield service.invoke('scopedTodo.fetch.byId', {ownerId:9, id:'12345678-1234-1234-1234-123456789012'})

		rec2.should.eql(rec3)
		rec3.done.should.be.true
	}))
})







































// ------------------------------------------------------------------------
//
// WITH mserv-validate
//
// ------------------------------------------------------------------------

describe('mserv-pgentity with mserv-validate', function(){

	let service  = mserv({amqp:false}).use('validate',validate).extend('entity',entity),
		postgres = service.ext.postgres()

	service.ext.entity('todo', {
		table:'todos',
		keys: {id:'uuid'},
		model: TodoModel,
		create: true,
		read: true,
		update: true,
		delete: true,
		merge: true
	})

	service.ext.entity('scopedTodo', {
		table:'scopedTodos',
		scope:'ownerId',
		keys: {id:'uuid'},
		model: ScopedTodoModel,
		create: true,
		read: true,
		update: true,
		delete: true,
		merge: true
	})


	before(function(done){
		postgres.none('create extension if not exists pgcrypto').then(function(){
			postgres.none('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text not null, done boolean default false, created_at timestamp default current_timestamp)').then(function(){
				postgres.none('create table if not exists scopedTodos (owner_id int, id  uuid not null primary key default gen_random_uuid(), name text not null, done boolean default false, created_at timestamp default current_timestamp)').nodeify(done)
			})
		})		
	})

	after(function(done){
		postgres.none('drop table todos; drop table scopedTodos').nodeify(done)
	})

	beforeEach(function(done){
		postgres.none('truncate table todos; truncate table scopedTodos').nodeify(done)
	})


	// ------------------------------------------------------------------------
	// Create
	// ------------------------------------------------------------------------

	it('create should return a record', wrappedTest(function*(){

		let rec1 = {name: 'item #1', done:false},
			rec2 = yield service.invoke('todo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)

		// Our model does not have the createdAt column even though the DB does.
		rec2.should.not.have.property.createdAt
	}))

	it('create should throw constraint violation', wrappedTest(function*(){

		try {
			// mserv-validate not installed
			yield service.invoke('todo.create', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	it('create should return multiple records', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false},{name: 'item #2', done:true}],
			recs2 = yield service.invoke('todo.create', {batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)
		recs2[1].name.should.equal(recs1[1].name)
		recs2[1].done.should.equal(recs1[1].done)
	}))



	it('create should return one record and one error', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false}, {done:true}],
			recs2 = yield service.invoke('todo.create', {batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)
		recs2[1].should.have.property.error$

	}))


	// ------------------------------------------------------------------------
	// Create Scoped
	// ------------------------------------------------------------------------

	it('scoped create should return a record', wrappedTest(function*(){

		let rec1 = {ownerId:1, name: 'item #1', done:false},
			rec2 = yield service.invoke('scopedTodo.create', rec1)
		
		should.exist(rec2)
		rec2.id.should.exist 

		// Check for equality of properties
		rec2.ownerId.should.equal(rec1.ownerId)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)

		// Our model does not have the createdAt column even though the DB does.
		rec2.should.not.have.property.createdAt
	}))

	it('scoped create should throw missing scope', wrappedTest(function*(){

		try {
			// mserv-validate not installed
			yield service.invoke('scopedTodo.create', {name:'item #2', done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	it('scoped create should return multiple records', wrappedTest(function*(){

		let recs1 = [{name: 'item #1', done:false},{name: 'item #2', done:true}],
			recs2 = yield service.invoke('scopedTodo.create', {ownerId:1, batch:recs1})

		should.exist(recs2)
		recs2.length.should.equal(recs1.length)

		recs2[0].ownerId.should.equal(1)
		recs2[0].name.should.equal(recs1[0].name)
		recs2[0].done.should.equal(recs1[0].done)

		recs2[1].ownerId.should.equal(1)
		recs2[1].name.should.equal(recs1[1].name)
		recs2[1].done.should.equal(recs1[1].done)
	}))



	// ------------------------------------------------------------------------
	// Read
	// ------------------------------------------------------------------------

	it('fetch.byId should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})
			rec2.should.eql(rec1)
	}))

	it('fetch.byId should return many records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:true}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false}),
			recs = yield service.invoke('todo.fetch.byId', {id:[rec1.id, rec2.id, rec3.id]})

		should.exist(recs)
		recs.length.should.equal(3)
		
		recs[0].name.should.equal(rec1.name)
		recs[1].name.should.equal(rec2.name)
		recs[2].name.should.equal(rec3.name)

		recs[0].done.should.equal(rec1.done)
		recs[1].done.should.equal(rec2.done)
		recs[2].done.should.equal(rec3.done)
	}))


	it('fetch.byId should throw invalid input syntax for uuid', wrappedTest(function*(){
		try {
			let rec1 = yield service.invoke('todo.fetch.byId', {id:'not-a-uuid'})
			throw new Error('Invoke did not throw')
		}
		catch(err){
			if (err.message === 'Invoke did not throw')
				throw err
			_.pick(err,'name','message').should.eql({
				name: 'Error',
				message: 'validationErrors'
			})
		}
	}))


	it('fetch should return many records', wrappedTest(function*(){

		yield postgres.none(`insert into todos (name, done) values ('item1',false),('item2',true),('item3',false)`)

		let recs = yield service.invoke('todo.fetch')
		should.exist(recs)
		recs.should.be.instanceOf(Array)
		recs.length.should.equal(3)		
	}))



	// ------------------------------------------------------------------------
	// Read Scoped
	// ------------------------------------------------------------------------

	it('scoped fetchById should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.fetch.byId', {ownerId:1, id:rec1.id})

		rec2.should.eql(rec1)
	}))

	it('scoped fetchById should return many records', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #2', done:true}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false}),
			recs = yield service.invoke('scopedTodo.fetch.byId', {ownerId:2, id:[rec1.id, rec2.id, rec3.id]})

		should.exist(recs)
		recs.length.should.equal(3)
		
		recs[0].name.should.equal(rec1.name)
		recs[1].name.should.equal(rec2.name)
		recs[2].name.should.equal(rec3.name)

		recs[0].done.should.equal(rec1.done)
		recs[1].done.should.equal(rec2.done)
		recs[2].done.should.equal(rec3.done)
	}))


	it('scoped fetchById should throw missingScope', wrappedTest(function*(){
		try {
			yield service.invoke('scopedTodo.fetch.byId', {id:'12345678-1234-1234-1234-123456789012'})
			throw new Error('Invoke did not throw')
		}
		catch(err){
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))

	it('scoped fetch should return many records', wrappedTest(function*(){

		yield postgres.none(`insert into scopedTodos (owner_id, name, done) values (1,'item1',false),(1,'item2',true),(2,'item3',false),(2,'item4',false)`)

		let recs = yield service.invoke('scopedTodo.fetch', {ownerId:1})

		should.exist(recs)
		recs.length.should.equal(2)
		recs[0].ownerId.should.equal(1)
		recs[1].ownerId.should.equal(1)
	}))

	it('scoped fetch throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.fetch')
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw')
				throw err
		}
	}))


	// ------------------------------------------------------------------------
	// Update
	// ------------------------------------------------------------------------

	it('update should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

		rec2.id.should.equal(rec1.id)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(true)
		rec2.should.not.have.property.createdAt
	}))


	it('update should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('todo.update', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('update should return multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		rec1.done = true,
		rec2.done = true,
		rec3.done = true

		let recs = yield service.invoke('todo.update', {batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[1].id.should.equal(rec2.id)
		recs[2].id.should.equal(rec3.id)

		recs[0].done.should.equal(true)
		recs[1].done.should.equal(true)
		recs[2].done.should.equal(true)
	}))

	it('update should return some records and some errors', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		rec1.done = true
		rec2.done = true
		rec2.name = null
		rec3.done = true	

		let recs = yield service.invoke('todo.update', {batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[2].id.should.equal(rec3.id)

		recs[0].done.should.equal(true)
		recs[2].done.should.equal(true)

		recs[1].should.have.property.error$
	}))


	// ------------------------------------------------------------------------
	// Update Scoped
	// ------------------------------------------------------------------------

	it('scoped update should return a record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.update', {ownerId:1, id:rec1.id, done:true})

		rec2.id.should.equal(rec1.id)
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(true)
		rec2.should.not.have.property.createdAt
	}))

	it('scoped update should throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.update', {id:'12345678-1234-1234-1234-123456789012', done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingScope ownerId')
				throw err
		}
	}))

	it('scoped update should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.update', {ownerId:3, done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('scoped update should return multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		rec1.done = true,
		rec2.done = true,
		rec3.done = true

		delete rec1.ownerId
		delete rec2.ownerId

		let recs = yield service.invoke('scopedTodo.update', {ownerId:1, batch:[rec1, rec2]})

		should.exist(recs)
		recs.length.should.equal(2)

		recs[0].ownerId.should.equal(1)
		recs[0].id.should.equal(rec1.id)
		recs[0].done.should.equal(true)

		recs[1].ownerId.should.equal(1)
		recs[1].id.should.equal(rec2.id)
		recs[1].done.should.equal(true)
	}))


	it('scoped update should return some records and some errors', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		rec1.done = true
		rec2.done = true
		rec2.name = null
		rec3.done = true	

		delete rec1.ownerId
		delete rec2.ownerId

		let recs = yield service.invoke('scopedTodo.update', {ownerId:1, batch:[rec1, rec2, rec3]})

		should.exist(recs)
		recs.length.should.equal(3)

		recs[0].id.should.equal(rec1.id)
		recs[0].done.should.equal(true)

		recs[1].should.have.property.error$
	}))	



	// ------------------------------------------------------------------------
	// Delete
	// ------------------------------------------------------------------------


	it('delete.byId should delete single records', wrappedTest(function*(){

		let rec1  = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			count = yield service.invoke('todo.delete.byId', {id:rec1.id})

		count.should.equal(1)
	}))


	it('delete.byId should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('todo.delete.byId', {done:false})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey') {
				console.error(err)
				throw err
			}
		}
	}))

	it('delete.byId should delete multiple records', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false}),
			rec3 = yield service.invoke('todo.create', {name: 'item #3', done:false})

		let count = yield service.invoke('todo.delete.byId', {id:[rec1.id, rec2.id, rec3.id]})

		count.should.equal(3)
	}))

	it('delete.byId should ignore unknown keys', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
			rec2 = yield service.invoke('todo.create', {name: 'item #2', done:false})

		let count = yield service.invoke('todo.delete.byId', {id:[
			rec1.id, 
			'12345678-1234-1234-1234-123456789012',
			rec2.id, 
			'12345678-1234-4567-8901-123456789012'
		]})

		count.should.equal(2)
	}))

	


	// ------------------------------------------------------------------------
	// Delete Scoped
	// ------------------------------------------------------------------------


	it('scoped delete.byId should delete single records', wrappedTest(function*(){

		let rec1  = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:rec1.id})

		count.should.equal(1)
	}))

	it('scoped delete.byId should throw missingScope', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.delete.byId', {id:'12345678-1234-1234-1234-123456789012'})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingScope ownerId')
				throw err
		}
	}))

	it('scoped delete.byId should throw missingKey', wrappedTest(function*(){

		try {
			yield service.invoke('scopedTodo.delete.byId', {ownerId:1})
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message === 'Invoke did not throw' || err.message !== 'missingKey')
				throw err
		}
	}))

	it('scoped delete.byId should only delete records within scope', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false}),
			rec4 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false}),
			rec5 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #3', done:false})

		let count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:[rec1.id, rec2.id, rec3.id, rec4.id, rec5.id]})
		count.should.equal(3)

		let recs = yield service.invoke('scopedTodo.fetch', {ownerId:2})
		recs.length.should.equal(2)
	}))

	it('scoped delete.byId should ignore unknown ids', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false})

		let count = yield service.invoke('scopedTodo.delete.byId', {ownerId:1, id:[
			rec1.id, 
			'12345678-1234-1234-1234-123456789012',
			rec2.id, 
			'12345678-1234-4567-8901-123456789012',
		]})

		count.should.equal(2)
	}))


	it('scoped delete.all should only delete records within scope', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #1', done:false}),
			rec2 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #2', done:false}),
			rec3 = yield service.invoke('scopedTodo.create', {ownerId:1, name: 'item #3', done:false}),
			rec4 = yield service.invoke('scopedTodo.create', {ownerId:2, name: 'item #1', done:false}),
			rec5 = yield service.invoke('scopedTodo.create', {ownerId:3, name: 'item #2', done:false})

		let count = yield service.invoke('scopedTodo.delete.all', {ownerId:1})

		count.should.equal(3)
	}))




	// ------------------------------------------------------------------------
	// Merge 
	// ------------------------------------------------------------------------

	it('merge should insert a new record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.merge', {id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.should.eql({id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
	}))


	it('merge should update the existing record', wrappedTest(function*(){

		let rec1 = yield service.invoke('todo.merge', {id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.done = true
		let rec2 = yield service.invoke('todo.merge', rec1),
			rec3 = yield service.invoke('todo.fetch.byId', {id:'12345678-1234-1234-1234-123456789012'})

		rec2.should.eql(rec3)
		rec3.done.should.be.true
	}))

	// ------------------------------------------------------------------------
	// Merge Scoped
	// ------------------------------------------------------------------------

	it('scoped merge should insert a new record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.merge', {ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.should.eql({ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
	}))


	it('scoped merge should update the existing record', wrappedTest(function*(){

		let rec1 = yield service.invoke('scopedTodo.merge', {ownerId:9, id:'12345678-1234-1234-1234-123456789012', name:'item #1', done:false})
		rec1.done = true
		let rec2 = yield service.invoke('scopedTodo.merge', rec1),
			rec3 = yield service.invoke('scopedTodo.fetch.byId', {ownerId:9, id:'12345678-1234-1234-1234-123456789012'})

		rec2.should.eql(rec3)
		rec3.done.should.be.true
	}))
})















