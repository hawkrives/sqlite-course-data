'use strict'

const Database = require('better-sqlite3')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const {performance} = require('perf_hooks')

function makeTables(db) {
	const makeNameTable = ({name}) => {
		let indexName = `${name}_name_uindex`

		// DROP TABLE IF EXISTS department
		db.prepare(`DROP TABLE IF EXISTS ${name}`).run()
		// CREATE TABLE IF NOT EXISTS department (id integer PRIMARY KEY AUTOINCREMENT, name text NOT NULL)
		db.prepare(
			`CREATE TABLE IF NOT EXISTS ${name} (
				id integer PRIMARY KEY AUTOINCREMENT,
				name text NOT NULL
			)`,
		).run()

		// DROP INDEX IF EXISTS department_name_uindex
		db.prepare(`DROP INDEX IF EXISTS ${indexName}`).run()
		// CREATE UNIQUE INDEX department_name_uindex ON department (name)
		db.prepare(`CREATE UNIQUE INDEX ${indexName} ON ${name} (name)`).run()
	}

	const makeContentTable = ({name}) => {
		let indexName = `${name}_content_uindex`

		// DROP TABLE IF EXISTS description
		db.prepare(`DROP TABLE IF EXISTS ${name}`).run()
		// CREATE TABLE IF NOT EXISTS description (id integer PRIMARY KEY AUTOINCREMENT, content text NOT NULL)
		db.prepare(
			`CREATE TABLE IF NOT EXISTS ${name} (
				id integer PRIMARY KEY AUTOINCREMENT,
				content text NOT NULL
			)`,
		).run()

		// DROP INDEX IF EXISTS description_content_uindex
		db.prepare(`DROP INDEX IF EXISTS ${indexName}`).run()
		// CREATE UNIQUE INDEX description_content_uindex ON description (content)
		db.prepare(`CREATE UNIQUE INDEX ${indexName} ON ${name} (content)`).run()
	}

	makeNameTable({name: 'department'})
	makeNameTable({name: 'instructor'})
	makeNameTable({name: 'gereq'})
	makeNameTable({name: 'location'})

	db.prepare(`DROP TABLE IF EXISTS time`).run()
	db.prepare(
		`CREATE TABLE IF NOT EXISTS time (
			id integer PRIMARY KEY AUTOINCREMENT,
			days text NOT NULL,
			start text NOT NULL,
			end text NOT NULL
		)`,
	).run()
	db.prepare('DROP INDEX IF EXISTS time_days_start_end_uindex').run()
	db.prepare(
		'CREATE UNIQUE INDEX time_days_start_end_uindex ON time (days, start, end)',
	).run()

	makeContentTable({name: 'description'})
	makeContentTable({name: 'note'})
	makeContentTable({name: 'prerequisite'})

	db.prepare(`DROP TABLE IF EXISTS course`).run()
	db.prepare(
		`CREATE TABLE IF NOT EXISTS course (
			id integer PRIMARY KEY AUTOINCREMENT,
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
		)`,
	).run()
}

function makeLinkingTables(db) {
	const makeLinkTable = ([tableA, tableB]) => {
		let tableName = `${tableA}_${tableB}`
		let columnA = `${tableA}_id`
		let columnB = `${tableB}_id`
		let fkA = `${tableName}_${columnA}_fk`
		let fkB = `${tableName}_${columnB}_fk`
		let indexName = `${tableName}_${columnA}_${columnB}_uindex`

		// DROP TABLE IF EXISTS course_department
		db.prepare(`DROP TABLE IF EXISTS ${tableName}`).run()

		// CREATE TABLE IF NOT EXISTS course_department (
		// 	id integer PRIMARY KEY AUTOINCREMENT,
		// 	course_id integer NOT NULL
		// 		constraint course_department_course_id_fk references course,
		// 	department_id integer NOT NULL
		// 		constraint course_department_department_id_fk references department
		// )
		db.prepare(
			`CREATE TABLE IF NOT EXISTS ${tableName} (
				id integer PRIMARY KEY AUTOINCREMENT,
				${columnA} integer NOT NULL
					constraint ${fkA} references ${tableA},
				${columnB} integer NOT NULL
					constraint ${fkB} references ${tableB}
			)`,
		).run()

		// DROP INDEX IF EXISTS course_department_course_id_department_id_uindex
		db.prepare(`DROP INDEX IF EXISTS ${indexName}`).run()

		// CREATE UNIQUE INDEX course_department_course_id_department_id_uindex ON course_department (course_id, department_id)
		db.prepare(
			`CREATE UNIQUE INDEX ${indexName} ON ${tableName} (
				${columnA},
				${columnB}
			)`,
		).run()
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

function prepareStatements(db) {
	let course_stmt = db.prepare(`
		INSERT INTO course (
			clbid, credits, crsid, level,
			name, number, pn, section,
			status, title, type,
			year, semester
		) VALUES (
			:clbid, :credits, :crsid, :level,
			:name, :number, :pn, :section,
			:status, :title, :type,
			:year, :semester
		)
	`)

	let course_note_stmt = db.prepare(`
		INSERT INTO course_note (course_id, note_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_department_stmt = db.prepare(`
		INSERT INTO course_department (course_id, department_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_gereq_stmt = db.prepare(`
		INSERT INTO course_gereq (course_id, gereq_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_description_stmt = db.prepare(`
		INSERT INTO course_description (course_id, description_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_instructor_stmt = db.prepare(`
		INSERT INTO course_instructor (course_id, instructor_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_location_stmt = db.prepare(`
		INSERT INTO course_location (course_id, location_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_time_stmt = db.prepare(`
		INSERT INTO course_time (course_id, time_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)
	let course_prerequisite_stmt = db.prepare(`
		INSERT INTO course_prerequisite (course_id, prerequisite_id)
		VALUES (?, ?)
		ON CONFLICT DO NOTHING
	`)

	let department_stmt = db.prepare(`
		INSERT INTO department (name)
		VALUES (:name)
		ON CONFLICT DO NOTHING
	`)
	let gereq_stmt = db.prepare(`
		INSERT INTO gereq (name)
		VALUES (:name)
		ON CONFLICT DO NOTHING
	`)
	let instructor_stmt = db.prepare(`
		INSERT INTO instructor (name)
		VALUES (:name)
		ON CONFLICT DO NOTHING
	`)
	let location_stmt = db.prepare(`
		INSERT INTO location (name)
		VALUES (:name)
		ON CONFLICT DO NOTHING
	`)
	let time_stmt = db.prepare(`
		INSERT INTO time (days, start, end)
		VALUES (:days, :start, :end)
		ON CONFLICT DO NOTHING
	`)
	let description_stmt = db.prepare(`
		INSERT INTO description (content)
		VALUES (:content)
		ON CONFLICT DO NOTHING
	`)
	let note_stmt = db.prepare(`
		INSERT INTO note (content)
		VALUES (:content)
		ON CONFLICT DO NOTHING
	`)
	let prerequisite_stmt = db.prepare(`
		INSERT INTO prerequisite (content)
		VALUES (:content)
		ON CONFLICT DO NOTHING
	`)

	let get_department_stmt = db.prepare(`
		SELECT id FROM department
		WHERE name = :name
	`)
	let get_gereq_stmt = db.prepare(`
		SELECT id FROM gereq
		WHERE name = :name
	`)
	let get_instructor_stmt = db.prepare(`
		SELECT id FROM instructor
		WHERE name = :name
	`)
	let get_location_stmt = db.prepare(`
		SELECT id FROM location
		WHERE name = :name
	`)
	let get_time_stmt = db.prepare(`
		SELECT id FROM time
		WHERE days = :days AND start = :start AND end = :end
	`)
	let get_description_stmt = db.prepare(`
		SELECT id FROM description
		WHERE content = :content
	`)
	let get_note_stmt = db.prepare(`
		SELECT id FROM note
		WHERE content = :content
	`)
	let get_prerequisite_stmt = db.prepare(`
		SELECT id FROM prerequisite
		WHERE content = :content
	`)

	return {
		select: {
			department: get_department_stmt,
			gereq: get_gereq_stmt,
			instructor: get_instructor_stmt,
			location: get_location_stmt,
			time: get_time_stmt,
			description: get_description_stmt,
			note: get_note_stmt,
			prerequisite: get_prerequisite_stmt,
		},
		insert: {
			course: course_stmt,
			course_note: course_note_stmt,
			course_department: course_department_stmt,
			course_gereq: course_gereq_stmt,
			course_description: course_description_stmt,
			course_instructor: course_instructor_stmt,
			course_location: course_location_stmt,
			course_time: course_time_stmt,
			course_prerequisite: course_prerequisite_stmt,
			department: department_stmt,
			gereq: gereq_stmt,
			instructor: instructor_stmt,
			location: location_stmt,
			time: time_stmt,
			description: description_stmt,
			note: note_stmt,
			prerequisite: prerequisite_stmt,
		},
	}
}

function processCourse(course, stmt) {
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

	let {lastInsertROWID: cid} = stmt.insert.course.run({
		clbid,
		credits,
		crsid,
		level: String(level),
		name,
		number: String(number),
		pn: pn ? 1 : 0,
		section,
		status,
		title,
		type,
		year,
		semester,
	})

	departments.forEach(department => {
		stmt.insert.department.run({name: department})
		let {id} = stmt.select.department.get({name: department})
		stmt.insert.course_department.run(cid, id)
	})

	gereqs.forEach(gereq => {
		stmt.insert.gereq.run({name: gereq})
		let {id: gereqId} = stmt.select.gereq.get({name: gereq})
		stmt.insert.course_gereq.run(cid, gereqId)
	})

	instructors.forEach(instructor => {
		stmt.insert.instructor.run({name: instructor})
		let {id} = stmt.select.instructor.get({name: instructor})
		stmt.insert.course_instructor.run(cid, id)
	})

	locations.forEach(location => {
		stmt.insert.location.run({name: location})
		let {id} = stmt.select.location.get({name: location})
		stmt.insert.course_location.run(cid, id)
	})

	notes.forEach(note => {
		stmt.insert.note.run({content: note})
		let {id} = stmt.select.note.get({content: note})
		stmt.insert.course_note.run(cid, id)
	})

	description.forEach(description => {
		stmt.insert.description.run({content: description})
		let {id} = stmt.select.description.get({content: description})
		stmt.insert.course_description.run(cid, id)
	})

	times.forEach(timestring => {
		let [days, time] = timestring.split(/\s+/)
		let [start, end] = time.split('-')

		stmt.insert.time.run({days, start, end})
		let {id} = stmt.select.time.get({days, start, end})
		stmt.insert.course_time.run(cid, id)
	})

	if (prerequisites) {
		stmt.insert.prerequisite.run({content: prerequisites})
		let {id} = stmt.select.prerequisite.get({content: prerequisites})
		stmt.insert.course_prerequisite.run(cid, id)
	}
}

function main() {
	// rimraf.sync('./courses.sqlite')
	let db = new Database('./courses.sqlite', {memory: true})

	let begin = db.prepare('BEGIN')
	let commit = db.prepare('COMMIT')
	let rollback = db.prepare('ROLLBACK')

	// Higher order function - returns a function that always runs in a transaction
	function asTransaction(func) {
		return (function(...args) {
			begin.run()
			try {
				func(...args)
				commit.run()
			} finally {
				if (db.inTransaction) {
					rollback.run()
				}
			}
		})()
	}

	prepareDb(db)

	let statements = prepareStatements(db)

	let dataFiles = fs
		.readdirSync('../data')
		.filter(filename => filename.endsWith('.json'))

	dataFiles.forEach(filename => {
		console.log(filename)
		let data = fs.readFileSync(path.join('..', 'data', filename), 'utf-8')
		let courses = JSON.parse(data)

		let start = performance.now()
		asTransaction(() => {
			courses.forEach(c => processCourse(c, statements))
		})

		let end = performance.now()
		console.log('processed', filename, 'in', (end - start).toFixed(2), 'ms')
	})
}

main()
