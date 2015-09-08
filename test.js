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

	



describe("mserv-pgentity\n", function(){


	// ------------------------------------------------------------------------
	//
	// Without validate
	//
	// ------------------------------------------------------------------------

	describe('without mserv-validate', function(){

		let service  = mserv({amqp:false}).extend('entity',entity),
			postgres = service.ext.postgres()

		service.ext.entity('todo', {
			table:'todos',
			keys: {id:'uuid'},
			model: TodoModel,
			create: true,
			read: true,
			update: true,
			delete: true
		})

		before(function(done){
			postgres.queryRaw('create extension if not exists pgcrypto').then(function(){
				postgres.queryRaw('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text, done boolean, created_at timestamp default current_timestamp)').nodeify(done)	
			})		
		})

		after(function(done){
			postgres.queryRaw('drop table todos').nodeify(done)
		})

		beforeEach(function(done){
			postgres.queryRaw('truncate table todos').nodeify(done)
		})


		it('should create a record', wrappedTest(function*(){

			let rec1 = {name: 'item #1', done:false},
				rec2 = yield service.invoke('todo.create', rec1)
				
			rec2.id.should.exist 
			rec2.name.should.equal(rec1.name)
			rec2.done.should.equal(rec1.done)
			rec2.should.not.have.property.createdAt
		}))


		it('should read the record', wrappedTest(function*(){

			let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
				rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})

			rec2.should.eql(rec1)
		}))

		it('should fail to read the record', wrappedTest(function*(){
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


		it('should update the record', wrappedTest(function*(){

			let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
				rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

			rec2.id.should.equal(rec1.id)
			rec2.name.should.equal(rec1.name)
			rec2.done.should.equal(true)
			rec2.should.not.have.property.createdAt
		}))


		it('should fail to update the record', wrappedTest(function*(){

			let rec1 = yield service.invoke('todo.update', {/* no id */ done:true})
			should.not.exist(rec1)
		}))
	})





	// ------------------------------------------------------------------------
	//
	// With validate
	//
	// ------------------------------------------------------------------------


	describe('with mserv-validate', function(){

		let service  = mserv({amqp:false}).use('validate',validate).extend('entity',entity),
			postgres = service.ext.postgres()

		service.ext.entity('todo', {
			table:'todos',
			keys: {id:'uuid'},
			model: TodoModel,
			create: true,
			read: true,
			update: true,
			delete: true
		})

		before(function(done){
			postgres.queryRaw('create extension if not exists pgcrypto').then(function(){
				postgres.queryRaw('create table if not exists todos (id  uuid not null primary key default gen_random_uuid(), name text, done boolean, created_at timestamp default current_timestamp)').nodeify(done)	
			})		
		})

		after(function(done){
			postgres.queryRaw('drop table todos').nodeify(done)
		})

		beforeEach(function(done){
			postgres.queryRaw('truncate table todos').nodeify(done)
		})


		it('should create a record', wrappedTest(function*(){

			let rec1 = {name: 'item #1', done:false},
				rec2 = yield service.invoke('todo.create', rec1)
				
			rec2.id.should.exist 
			rec2.name.should.equal(rec1.name)
			rec2.done.should.equal(rec1.done)
			rec2.should.not.have.property.createdAt
		}))


		it('should read the record', wrappedTest(function*(){

			let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
				rec2 = yield service.invoke('todo.fetch.byId', {id:rec1.id})

			rec2.should.eql([rec1])
		}))

		it('should fail to read the record', wrappedTest(function*(){
			try {
				let rec1 = yield service.invoke('todo.fetch.byId', {id:'not-a-uuid'})
				throw new Error('Invoke did not throw')
			}
			catch(err){
				if (err.message === 'Invoke did not throw')
					throw err

				_.pick(err,'name','message','errors').should.eql({
					name: 'Error',
					message: 'validationErrors',
					errors: [{key:'id.0',value:'not-a-uuid',error:'notGUID'}]
				})
			}
		}))


		it('should update the record', wrappedTest(function*(){

			let rec1 = yield service.invoke('todo.create', {name: 'item #1', done:false}),
				rec2 = yield service.invoke('todo.update', {id:rec1.id, done:true})

			rec2.id.should.equal(rec1.id)
			rec2.name.should.equal(rec1.name)
			rec2.done.should.equal(true)
			rec2.should.not.have.property.createdAt
		}))


		it('should fail to update the record', wrappedTest(function*(){
			try {
				let rec1 = yield service.invoke('todo.update', {/* no id */ done:true})
				throw new Error('Invoke did not throw')
			}
			catch(err) {
				if (err.message === 'Invoke did not throw')
					throw err

				_.pick(err,'name','message','errors').should.eql({
					name:'Error',
					message:'validationErrors',
					errors: [{key:'id',value:undefined,error:'required'}]
				})
			}
		}))
	})
})





