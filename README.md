# NOT MAINTAINED ANYMORE!
If you would like to own this project I can transfer it to you along with all the other related projects.

# Introduction
mserv-pgentity is [mserv](https://github.com/macprog-guy/mserv) extension that simplifies creating CRUD like actions for postgres-backed data entities. The middleware assumes that [mserv-validate](https://github.com/macprog-guy/mserv-validate) is present and the postgres client is [pg-promise](https://github.com/vitaly-t/pg-promise).

# Installation

	$ npm i --save mserv-pgentity

# Usage

```js

var entity  = require('mserv-pgentity'),
	service = require('mserv')(),

service.extend('enitity', entity, {
	postgres: 'postgres://localhost/mydatabase'
})

service.ext.entity('todo', {
	table: 'todos',
	scope: 'userId',
	keys: {id:'uuid'}
	model: {
		userId: Joi.string().guid(),
		id:     Joi.string().guid(),
		seq:    Joi.number(),
		title:  Joi.string(),
		done:   Joi.boolean(),
	},
	create: true,
	read:   true,
	update: true,
	delete: true
})


service.invoke('todo.fetch.all', {ownerId:'12345678-1234-1234-1234-123456789012'})


```

# Options

The extension accepts only one option:

- `postgres`: URI to the postgres database. If the context middleware is installed the postgres property will be added to the context.

# Entities

When creating entities there are more options:

- `name`  : prefix for the action names that are created.
- `table` : postgres table name with schema prefix if needed.
- `scope` : multi-tenant support throw scoping.
- `keys`  : object with key names and their types.
- `model` : object with column names (camel-cased) and their Joi model.
- `create` : if true the `<name>.create` action will be created. Can also be a generator function, which acts as middleware around the create action. The generator function should have the following signature: `function*(array[Object], next)` and `yield next(array)` where array is the possibly modified array of objects that was initially passed in.
- `read`   : if true the `<name>.find.by<Key>` actions will be created for each key. Can also be a generator function, which acts as middleware around the read action. See `create` for more details.
- `update` : if true the `<name>.update` action will be created. Can also be a generator function, which acts as middleware around the update action. See `create` for more details.
- `delete` : if true the `<name>.delete` action will be created. Can also be a generator function, which acts as middleware around the delete action. See `create` for more details.
- `merge` : if true the `<name>.merge` action will be created. Can also be a generator function, which acts as middleware around the merge action. Merge attempts perform update and will insert if the update failed. See `create` for more details.


# Actions

The following actions are generated for you when using mserv-pgentity. Note that all functions support
both a single and batch modes. 

- `<name>.create` : In single mode the argument should be an object that satisfies `model`. The result will be the created object or an exception will be thrown. In batch mode, the argument should be an object with a `batch` key whose value is an array of objects satisfying `model`. Returns an array of objects. Objects that could not be created will return `{error$}` with some details.

- `<name>.fetch.by<Key>` : There will be one such action for each specified key. The argument would be an object with the `<key>` field that is either a single value or an array of values (single or batch). The result is either null, a single object or an array of objects.

- `<name>.fetch` :  Takes no arguments (or a scope argument) and returns all of the records.


- `<name>.update` :  In single mode the argument should be an object that satisfies `model`. The result will be the updated object or an exception will be thrown. In batch mode, the argument should be an object with a `batch` key whose value is an array of objects satisfying `model`. Returns an array of objects. Objects that could not be created will return `{error$}` with some details.

- `<name>.delete.by<Key>` :  There will be one such action for each specified key. The argument would be an object with the `<key>` field that is either a single value or an array of values (single or batch). The result is always the number of deleted records.


- `<name>.delete.all<Key>` : Only if scoped, Takes no arguments and deletes all of the records within the scope. The result is the number of deleted records.



# Scoping

Scoping was introduced to facilitate multi-tenancy. When scoped, all actions will require a key whose name is what was specified in the scope. For example if `scope: 'tenantId'` then all actions will require a `tenantId`. If the scope key is not present then a `missingScope` exception is thrown.

