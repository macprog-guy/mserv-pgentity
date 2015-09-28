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
	id:   Joi.string().guid(),
	name: Joi.string(),
	done: Joi.boolean()
}

var ScopedTodoModel = {	
	ownerId: Joi.number(),
	id:   Joi.number(),
	name: Joi.string(),
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
// Without validate
//
// ------------------------------------------------------------------------

// describe('mserv-pgentity without mserv-validate', function(){

// 	let service  = mserv({amqp:false}).extend('entity',entity),
// 		postgres = service.ext.postgres()

// 	service.ext.entity('todo', {
// 		table:'todos',
// 		keys: {id:'uuid'},
// 		model: TodoModel,
// 		create: true,
// 		read: true,
// 		update: true,
// 		delete: true
// 	})

// 	before(function(done){
// 		postgres.queryRaw('create extension if not exists pgcrypto').then(function(){
// 			postgres.queryRaw('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text, done boolean, created_at timestamp default current_timestamp)').nodeify(done)	
// 		})		
// 	})

// 	after(function(done){
// 		postgres.queryRaw('drop table todos').nodeify(done)
// 	})

// 	beforeEach(function(done){
// 		postgres.queryRaw('truncate table todos').nodeify(done)
// 	})


// 	it('should create a record', wrappedTest(function*(){

// 		let rec1 = {name: 'item #1', done:false},
// 			rec2 = yield service.invoke('todo.create', rec1)
			
// 		rec2.id.should.exist 
// 		rec2.name.should.equal(rec1.name)
// 		rec2.done.should.equal(rec1.done)
// 		rec2.should.not.have.property.createdAt
// 	}))


// 	it('should read the record', wrappedTest(function*(){

// 		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
// 			rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})

// 		rec2.should.eql(rec1)
// 	}))

// 	it('should fail to read the record', wrappedTest(function*(){
// 		try {
// 			let rec1 = yield service.invoke('todo.fetch.byId', {id:'not-a-uuid'})
// 			throw new Error('Invoke did not throw')
// 		}
// 		catch(err){
// 			if (err.message === 'Invoke did not throw')
// 				throw err
// 			_.pick(err,'name','message').should.eql({
// 				name: 'error',
// 				message: 'invalid input syntax for uuid: "not-a-uuid"'
// 			})
// 		}
// 	}))


// 	it('should update the record', wrappedTest(function*(){

// 		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
// 			rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

// 		rec2.id.should.equal(rec1.id)
// 		rec2.name.should.equal(rec1.name)
// 		rec2.done.should.equal(true)
// 		rec2.should.not.have.property.createdAt
// 	}))


// 	it('should fail to update the record', wrappedTest(function*(){

// 		let rec1 = yield service.invoke('todo.update', {/* no id */ done:true})
// 		should.not.exist(rec1)
// 	}))
// })





// ------------------------------------------------------------------------
//
// With validate
//
// ------------------------------------------------------------------------


// describe('mserv-pgentity with mserv-validate', function(){

// 	let service  = mserv({amqp:false}).use('validate',validate).extend('entity',entity),
// 		postgres = service.ext.postgres()

// 	service.ext.entity('todo', {
// 		table:'todos',
// 		keys: {id:'uuid'},
// 		model: TodoModel,
// 		create: true,
// 		read: true,
// 		update: true,
// 		delete: true
// 	})

// 	before(function(done){
// 		postgres.queryRaw('create extension if not exists pgcrypto').then(function(){
// 			postgres.queryRaw('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text, done boolean, created_at timestamp default current_timestamp)').nodeify(done)	
// 		})		
// 	})

// 	after(function(done){
// 		postgres.queryRaw('drop table todos').nodeify(done)
// 	})

// 	beforeEach(function(done){
// 		postgres.queryRaw('truncate table todos').nodeify(done)
// 	})


// 	it('should create a record', wrappedTest(function*(){

// 		let rec1 = {name: 'item #1', done:false},
// 			rec2 = yield service.invoke('todo.create', rec1)
			
// 		rec2.id.should.exist 
// 		rec2.name.should.equal(rec1.name)
// 		rec2.done.should.equal(rec1.done)
// 		rec2.should.not.have.property.createdAt
// 	}))


// 	it('should read the record', wrappedTest(function*(){

// 		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
// 			rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})

// 		rec2.should.eql([rec1])
// 	}))

// 	it('should fail to read the record', wrappedTest(function*(){
// 		try {
// 			let rec1 = yield service.invoke('todo.fetch.byId', {id:'not-a-uuid'})
// 			throw new Error('Invoke did not throw')
// 		}
// 		catch(err){
// 			if (err.message === 'Invoke did not throw')
// 				throw err

// 			_.pick(err,'name','message','errors').should.eql({
// 				name: 'Error',
// 				message: 'validationErrors',
// 				errors: [{key:'id.0',value:'not-a-uuid',error:'notGUID'}]
// 			})
// 		}
// 	}))


// 	it('should update the record', wrappedTest(function*(){

// 		let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
// 			rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

// 		rec2.id.should.equal(rec1.id)
// 		rec2.name.should.equal(rec1.name)
// 		rec2.done.should.equal(true)
// 		rec2.should.not.have.property.createdAt
// 	}))


// 	it('should fail to update the record', wrappedTest(function*(){
// 		try {
// 			let rec1 = yield service.invoke('todo.update', {/* no id */ done:true})
// 			throw new Error('Invoke did not throw')
// 		}
// 		catch(err) {
// 			if (err.message === 'Invoke did not throw')
// 				throw err

// 			_.pick(err,'name','message','errors').should.eql({
// 				name:'Error',
// 				message:'validationErrors',
// 				errors: [{key:'id',value:undefined,error:'required'}]
// 			})
// 		}
// 	}))
// })



// ------------------------------------------------------------------------
//
// With scoping
//
// ------------------------------------------------------------------------



describe('mserv-pgentity with tenant scoping', function(){

	let service  = mserv({amqp:false}).extend('entity',entity),
		postgres = service.ext.postgres()

	service.ext.entity('todo', {
		table:'todos',
		scope: 'ownerId',
		keys: {id:'int'},
		model: TodoModel,
		create: true,
		read: true,
		update: true,
		delete: true
	})

	before(function(done){
		postgres.queryRaw('create extension if not exists pgcrypto').then(function(){
			postgres.queryRaw('create table if not exists todos (ownerId int not null, id  int not null, name text, done boolean default false, created_at timestamp default current_timestamp)').nodeify(done)	
		})		
	})

	after(function(done){
		postgres.queryRaw('drop table todos').nodeify(done)
	})

	beforeEach(function(done){
		postgres.queryRaw('truncate table todos').nodeify(done)
	})


	// ------------------------------------------------------------------------
	// Create
	// ------------------------------------------------------------------------


	it('should create a record', wrappedTest(function*(){

		let rec1 = {ownerId:1, id: 1, name: 'item #1', done:false},
			rec2 = yield service.invoke('todo.create', rec1)
			
		rec2.id.should.exist 
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)
		rec2.should.not.have.property.createdAt
	}))

	it('should throw a missing scope exception when inserting without scope', wrappedTest(function*(){

		try {
			let rec1 = {id: 1, name: 'item #1', done:false},
				rec2 = yield service.invoke('todo.create', rec1)
	
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message !== 'Invoke did not throw' && err.message !== 'missingScope ownerId')
				throw err
		}			
	}))


	// ------------------------------------------------------------------------
	// Read
	// ------------------------------------------------------------------------

	it('should return only one records', wrappedTest(function*(){

		yield postgres.queryRaw('insert into todos (ownerId, id, name) values ' +
			[	
				"(1, 1, 'Item #1')", 
				"(1, 2, 'Item #2')",
				"(1, 3, 'Item #3')",
				"(2, 1, 'Item #1')", 
				"(2, 2, 'Item #2')",
				"(2, 3, 'Item #3')",

			].join(','))

		let rec = yield service.invoke('todo.fetch.byId', {ownerId:1, id:2})

		should.exist(rec)
		rec.should.have.property.id

	}))

	it('should return only three records', wrappedTest(function*(){

		yield postgres.queryRaw('insert into todos (ownerId, id, name) values ' +
			[	
				"(1, 1, 'Item #1')", 
				"(1, 2, 'Item #2')",
				"(1, 3, 'Item #3')",
				"(2, 1, 'Item #1')", 
				"(2, 2, 'Item #2')",
				"(2, 3, 'Item #3')",

			].join(','))

		let recs = yield service.invoke('todo.fetch.all', {ownerId:1})

		should.exist(recs)
		recs.length.should.equal(3)

	}))


	it('should throw missing scope when reading without scope', wrappedTest(function*(){

		try {
			yield service.invoke('todo.fetch.byId', {id:1})	
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message !== 'Invoke did not throw' && err.message !== 'missingScope ownerId')
				throw err
		}
	}))


	// ------------------------------------------------------------------------
	// Update
	// ------------------------------------------------------------------------

	it('should update only one record', wrappedTest(function*(){

		yield postgres.queryRaw('insert into todos (ownerId, id, name) values ' +
			[	
				"(1, 1, 'Item #1')", 
				"(1, 2, 'Item #2')",
				"(1, 3, 'Item #3')",
				"(2, 1, 'Item #1')", 
				"(2, 2, 'Item #2')",
				"(2, 3, 'Item #3')",

			].join(','))

		let rec1 = {ownerId:1, id: 1, name: 'The First', done:true},
			rec2 = yield service.invoke('todo.update', rec1)
			
		rec2.id.should.exist 
		rec2.name.should.equal(rec1.name)
		rec2.done.should.equal(rec1.done)

		let count = yield postgres.count('todos', ['done=true'])

		count.should.equal(1)
	}))


	it('should throw a missing scope when updating without scope', wrappedTest(function*(){

		try {
			let rec1 = {id: 1, name: 'The First', done:true},
				rec2 = yield service.invoke('todo.update', rec1)
	
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message !== 'Invoke did not throw' && err.message !== 'missingScope ownerId')
				throw err
		}			

	}))


	// ------------------------------------------------------------------------
	// Delete
	// ------------------------------------------------------------------------

	it('should delete only one record', wrappedTest(function*(){

		yield postgres.queryRaw('insert into todos (ownerId, id, name) values ' +
			[	
				"(1, 1, 'Item #1')", 
				"(1, 2, 'Item #2')",
				"(1, 3, 'Item #3')",
				"(2, 1, 'Item #1')", 
				"(2, 2, 'Item #2')",
				"(2, 3, 'Item #3')",

			].join(','))

		yield service.invoke('todo.delete', {ownerId:1, id:1})
			
		let count = yield postgres.count('todos')

		count.should.equal(5)
	}))


	it('should throw a missing scope when deleting without scope', wrappedTest(function*(){

		try {
			yield service.invoke('todo.delete', {id:1})	
			throw new Error('Invoke did not throw')
		}
		catch(err) {
			if (err.message !== 'Invoke did not throw' && err.message !== 'missingScope ownerId')
				throw err
		}			

	}))



})