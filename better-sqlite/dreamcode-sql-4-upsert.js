'use strict'

const Database = require('better-sqlite3')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const makeSqlTag = require('./sql-tag')
const {performance} = require('perf_hooks')

async function makeTables(sql) {
	const makeNameTable = ({name}) => {
		let indexName = `${name}_name_uindex`

		// DROP TABLE IF EXISTS department
		sql.raw`DROP TABLE IF EXISTS ${name}`.run()
		// CREATE TABLE IF NOT EXISTS department (id integer PRIMARY KEY AUTOINCREMENT, name text NOT NULL)
		sql.raw`CREATE TABLE IF NOT EXISTS ${name} (id integer PRIMARY KEY AUTOINCREMENT, name text NOT NULL)`.run()

		// DROP INDEX IF EXISTS department_name_uindex
		sql.raw`DROP INDEX IF EXISTS ${indexName}`.run()
		// CREATE UNIQUE INDEX department_name_uindex ON department (name)
		sql.raw`CREATE UNIQUE INDEX ${indexName} ON ${name} (name)`.run()
	}

	const makeContentTable = ({name}) => {
		let indexName = `${name}_content_uindex`

		// DROP TABLE IF EXISTS description
		sql.raw`DROP TABLE IF EXISTS ${name}`.run()
		// CREATE TABLE IF NOT EXISTS description (id integer PRIMARY KEY AUTOINCREMENT, content text NOT NULL)
		sql.raw`CREATE TABLE IF NOT EXISTS ${name} (id integer PRIMARY KEY AUTOINCREMENT, content text NOT NULL)`.run()

		// DROP INDEX IF EXISTS description_content_uindex
		sql.raw`DROP INDEX IF EXISTS ${indexName}`.run()
		// CREATE UNIQUE INDEX description_content_uindex ON description (content)
		sql.raw`CREATE UNIQUE INDEX ${indexName} ON ${name} (content)`.run()
	}

	makeNameTable({name: 'department'})
	makeNameTable({name: 'instructor'})
	makeNameTable({name: 'gereq'})
	makeNameTable({name: 'location'})

	sql.raw`DROP TABLE IF EXISTS time`.run()
	sql.raw`CREATE TABLE IF NOT EXISTS time (
		id integer PRIMARY KEY AUTOINCREMENT,
		days text NOT NULL,
		start text NOT NULL,
		end text NOT NULL
	)`.run()
	sql.raw`DROP INDEX IF EXISTS time_days_start_end_uindex`.run()
	sql.raw`CREATE UNIQUE INDEX time_days_start_end_uindex ON time (days, start, end)`.run()

	makeContentTable({name: 'description'})
	makeContentTable({name: 'note'})
	makeContentTable({name: 'prerequisite'})

	sql.raw`DROP TABLE IF EXISTS course`.run()
	sql.raw`CREATE TABLE IF NOT EXISTS course (
		rowid integer PRIMARY KEY AUTOINCREMENT,
		clbid integer NOT NULL,
		credits real,
		crsid integer,
		level text,
		name text,
		number integer,
		pn boolean,
		section text,
		status text,
		title text,
		type text,

		year integer not null,
		semester integer not null
	)`.run()
}

function makeLinkingTables(sql) {
	const makeLinkTable = ([tableA, tableB]) => {
		let tableName = `${tableA}_${tableB}`
		let columnA = `${tableA}_id`
		let columnB = `${tableB}_id`
		let fkA = `${tableName}_${columnA}_fk`
		let fkB = `${tableName}_${columnB}_fk`
		let indexName = `${tableName}_${columnA}_${columnB}_uindex`

		// DROP TABLE IF EXISTS course_department
		sql.raw`DROP TABLE IF EXISTS ${tableName}`.run()

		// CREATE TABLE IF NOT EXISTS course_department (
		// 	id integer PRIMARY KEY AUTOINCREMENT,
		// 	course_id integer NOT NULL
		// 		constraint course_department_course_id_fk references course,
		// 	department_id integer NOT NULL
		// 		constraint course_department_department_id_fk references department
		// )
		sql.raw`CREATE TABLE IF NOT EXISTS ${tableName} (
			id integer PRIMARY KEY AUTOINCREMENT,
			${columnA} integer NOT NULL
				constraint ${fkA} references ${tableA},
			${columnB} integer NOT NULL
				constraint ${fkB} references ${tableB}
		)`.run()

		// DROP INDEX IF EXISTS course_department_course_id_department_id_uindex
		sql.raw`DROP INDEX IF EXISTS ${indexName}`.run()

		// CREATE UNIQUE INDEX course_department_course_id_department_id_uindex ON course_department (course_id, department_id)
		sql.raw`CREATE UNIQUE INDEX ${indexName} ON ${tableName} (${columnA}, ${columnB})`.run()
	}

	makeLinkTable(['course', 'department'])
	makeLinkTable(['course', 'instructor'])
	makeLinkTable(['course', 'gereq'])
	makeLinkTable(['course', 'location'])
	makeLinkTable(['course', 'time'])
	makeLinkTable(['course', 'note'])
	makeLinkTable(['course', 'description'])
	makeLinkTable(['course', 'prerequisite'])
}

function prepareDb(db) {
	makeTables(db)
	makeLinkingTables(db)
}

function insertCourse(sql, course) {
	let {
		clbid,
		credits,
		crsid,
		level,
		name,
		number,
		pn = null,
		section = null,
		status,
		title = null,
		type,
		year,
		semester,
		departments = [],
		gereqs = [],
		instructors = [],
		locations = [],
		notes = [],
		description = [],
		times = [],
		prerequisites = null,
	} = course

	let {lastInsertROWID: cid} = sql.run`
		INSERT INTO course (clbid, credits, crsid, level, name, number, pn, section, status, title, type, year, semester)
		VALUES (
			${clbid},
			${credits},
			${crsid},
			${String(level)},
			${name},
			${String(number)},
			${pn ? 1 : 0},
			${section},
			${status},
			${title},
			${type},
			${year},
			${semester}
		)
	`

	departments.forEach(name => {
		sql.run`INSERT INTO department (name) VALUES (${name}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM department WHERE name = ${name}`
		sql.run`INSERT INTO course_department (course_id, department_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	gereqs.forEach(name => {
		sql.run`INSERT INTO gereq (name) VALUES (${name}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM gereq WHERE name = ${name}`
		sql.run`INSERT INTO course_gereq (course_id, gereq_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	instructors.forEach(name => {
		sql.run`INSERT INTO instructor (name) VALUES (${name}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM instructor WHERE name = ${name}`
		sql.run`INSERT INTO course_instructor (course_id, instructor_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	locations.forEach(name => {
		sql.run`INSERT INTO location (name) VALUES (${name}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM location WHERE name = ${name}`
		sql.run`INSERT INTO course_location (course_id, location_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	notes.forEach(content => {
		sql.run`INSERT INTO note (content) VALUES (${content}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM note WHERE content = ${content}`
		sql.run`INSERT INTO course_note (course_id, note_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	description.forEach(content => {
		sql.run`INSERT INTO description (content) VALUES (${content}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`SELECT id FROM description WHERE content = ${content}`
		sql.run`INSERT INTO course_description (course_id, description_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	times.forEach(timestring => {
		let [days, time] = timestring.split(/\s+/)
		let [start, end] = time.split('-')

		sql.run`INSERT INTO time (days, start, end) VALUES (${days}, ${start}, ${end}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`
			SELECT id FROM time WHERE days = ${days} AND start = ${start} AND end = ${end}
		`
		sql.run`INSERT INTO course_time (course_id, time_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	})

	if (prerequisites) {
		sql.run`INSERT INTO prerequisite (content) VALUES (${prerequisites}) ON CONFLICT DO NOTHING`
		let {id} = sql.get`
			SELECT id FROM prerequisite WHERE content = ${prerequisites}
		`
		sql.run`INSERT INTO course_prerequisite (course_id, prerequisite_id) VALUES (${cid}, ${id}) ON CONFLICT DO NOTHING`
	}
}

function main() {
	rimraf.sync('./courses-2.sqlite')
	let db = new Database('./courses-2.sqlite')
	let sql = makeSqlTag(db)


	// Higher order function - returns a function that always runs in a transaction
	function asTransaction(func) {
		return (() => {
			sql.run`BEGIN`
			try {
				func()
				sql.run`COMMIT`
			} finally {
				if (db.inTransaction) {
					sql.run`ROLLBACK`
				}
			}
		})()
	}

	prepareDb(sql)

	let dataFiles = fs
		.readdirSync('../data')
		.filter(filename => filename.endsWith('.json'))
		.slice(0, 1)

	dataFiles.forEach(filename => {
		console.log(filename)
		let data = fs.readFileSync(path.join('..', 'data', filename), 'utf-8')
		let courses = JSON.parse(data)

		let start = performance.now()
		asTransaction(() => {
			courses.forEach(c => insertCourse(sql, c))
		})
		let end = performance.now()
		console.log('processed', filename, 'in', (end - start).toFixed(2), 'ms')
	})
}

function query() {
	let db = new Database('./courses-2.sqlite')
	let sql = makeSqlTag(db)

	// console.log(sql.counts())

	// console.log(sql.get`SELECT * FROM course WHERE credits = 0`)

	console.log('prepare, no params')
	console.log(db.prepare(`SELECT id FROM department WHERE name IN ('ASIAN')`).get())

	console.log('prepare, ye params')
	console.log(db.prepare(`SELECT id FROM department WHERE name IN (?)`).get(['ASIAN']))

	console.log('tag, no params')
	console.log(sql.get`SELECT id FROM department WHERE name IN ('ASIAN')`)

	console.log('tag, ye params')
	let x = ['ASIAN', 1, 'REL']
	console.log(x)
	console.log(sql.all`SELECT id FROM department WHERE name IN (${x})`)

	let y = 'ASIAN'
	console.log(sql.all`SELECT id FROM department WHERE name IN (${y})`)
}

// console.profile("dreamcode");
main()

// query()
// console.profileEnd();
