const QuickLRU = require('quick-lru')
const lodash = require('lodash')

function makeSqlTag(database) {
	const lru = new QuickLRU({maxSize: 1000})

	// CREATE TABLE IF NOT EXISTS ${tableName} (
	// 	id integer PRIMARY KEY AUTOINCREMENT,
	// 	${columnA} integer NOT NULL
	// 		constraint ${fkA} references ${tableA},
	// 	${columnB} integer NOT NULL
	// 		constraint ${fkB} references ${tableB}
	// )

	const raw = (strings, ...expressions) => {
		let statement = lodash.flatten(lodash.zip(strings, expressions))
		statement = statement.join('')

		return {
			statement,
			run: (...expressions) => run([statement], ...expressions),
			all: (...expressions) => all([statement], ...expressions),
			get: (...expressions) => get([statement], ...expressions),
			iter: (...expressions) => iter([statement], ...expressions),
		}
	}

	const prepareStatement = (strings, ...expressions) => {
		// console.log(strings, expressions)

		let result = ''
		for (let i = 0; i < strings.length; i++) {
			result += strings[i]

			let expr = expressions[i]
			if (expr !== undefined) {
				if (Array.isArray(expr)) {
					for (let j = 0; j < expr.length; j++) {
						result += '?'
						if (j < expr.length - 1) {
							result += ', '
						}
					}
				} /* else if (expr instanceof Set) {
					let asArr = [...expr]
					for (let j = 0; j < asArr.length; j++) {
						result += '?'
						if (j < asArr.length - 1) {
							result += ', '
						}
					}
				} */ else {
					result += '?'
				}
			}
		}

		return result
		// return strings.join('?')
	}

	let hit = 0, miss = 0
	const prepare = (strings, ...expressions) => {
		let statement = prepareStatement(strings, ...expressions)

		// console.log(statement, expressions)

		let prepared
		if (lru.has(statement)) {
			hit += 1
			prepared = lru.get(statement)
		} else {
			miss += 1
			prepared = database.prepare(statement)
			lru.set(statement, prepared)
		}

		return prepared
	}

	const run = (strings, ...expressions) => prepare(strings, ...expressions).run(expressions)
	const get = (strings, ...expressions) => {
		// console.log(prepare(strings, ...expressions))
		// console.log(...expressions)
		return prepare(strings, ...expressions).get(...expressions)
	}
	const all = (strings, ...expressions) => prepare(strings, ...expressions).all(...expressions)
	const iter = (strings, ...expressions) => prepare(strings, ...expressions).iter(expressions)

	const counts = () => { return {hit, miss} }

	const rawPrepare = (strings, ...expressions) => {
		let statement = lodash.flatten(lodash.zip(strings, expressions))
		statement = statement.join('')
		return database.prepare(statement)
	}

	return {raw, run, get, all, iter, prepare: rawPrepare, counts}
}

function test() {
	// database.prepare(statement :with :named :slots)
	// database.{run,get,iter,all}({keyed: params})

	// database.prepare(statement ? ? ?)
	// database.{run,get,iter,all}([array, of, params])

	let sql = makeSqlTag({prepare: x => ({run: x => x, all: x => [], get: x => x})})
//
// 	let [tableA, tableB] = ['course', 'department']
// 	let tableName = `${tableA}_${tableB}`
// 	let columnA = `${tableA}_id`
// 	let columnB = `${tableB}_id`
// 	let fkA = `${tableName}_${columnA}_fk`
// 	let fkB = `${tableName}_${columnB}_fk`
// 	let indexName = `${tableName}_${columnA}_${columnB}_uindex`
//
// 	let str = sql.raw`CREATE TABLE IF NOT EXISTS ${tableName} (
// 		id integer PRIMARY KEY AUTOINCREMENT,
// 		${columnA} integer NOT NULL
// 			constraint ${fkA} references ${tableA},
// 		${columnB} integer NOT NULL
// 			constraint ${fkB} references ${tableB}
// 	)`
//
// 	console.log(str)
//
// 	console.log(str.run)

	// let cid = 13456
	// let id = 2
	// let str2 = sql.run`INSERT OR IGNORE INTO course_department (course_id, department_id) VALUES (${cid}, ${id})`

	// console.log(str2)

	// let x = ['ASIAN', 'REL']
	// console.log(sql.get`SELECT id FROM department WHERE id IN (${x})`)

	console.log(sql.get`SELECT id FROM department WHERE id IN (${[[1, 2]]})`)
}

// test()

module.exports = makeSqlTag
