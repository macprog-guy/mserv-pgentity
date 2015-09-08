# Introduction
mserv-pgentity is [mserv](https://github.com/macprog-guy/mserv) extension that simplifies creating CRUD like actions for postgres-backed data entities. The middleware assumes that [mserv-validate](https://github.com/macprog-guy/mserv-validate) is present and the postgres client is [pg-promise](https://github.com/vitaly-t/pg-promise).

# Installation

	$ npm i --save mserv-validate

# Usage

```js

var entity  = require('mserv-pgentity'),
	service = require('mserv')(),

service.extend('enitity', entity, {
	postgres: 'postgres://localhost/mydatabase'
})

service.ext.entity({
	name: 'todo',
	table: 'todos',
	keys: {id:'uuid'}
	model: {
		id:    Joi.string().guid(),
		seq:   Joi.number(),
		title: Joi.string(),
		done:  Joi.boolean(),
	},
	create: true,
	read:   true,
	update: true,
	delete: true
})

```

# Options

The extension accepts only one option:

- `postgres`: URI to the postgres database. If the context middleware is installed the postgres property will be added to the context.

# Entities

When creating entities there are more options:

- `name`  : prefix for the action names that are created.
- `table` : postgres table name with schema prefix if needed.
- `keys`  : object with key names and their types.
- `model` : object with column names (camel-cased) and their Joi model.
- `create` : if true the `<name>.create` action will be created.
- `read`   : if true the `<name>.find.by<Key>` actions will be created for each key.
- `update` : if true the `<name>.update` action will be created.
- `delete` : if true the `<name>.delete` action will be created.

