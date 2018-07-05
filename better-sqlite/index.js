'use strict'

const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');
const rimraf = require('rimraf');

function prepareDb(db) {
	db.prepare(`
		CREATE TABLE department (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX department_name_uindex ON department (name)").run()
	db.prepare(`
		CREATE TABLE instructor (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX instructor_name_uindex ON instructor (name)").run()
	db.prepare(`
		CREATE TABLE gereq (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX gereq_name_uindex ON gereq (name)").run()
	db.prepare(`
		CREATE TABLE location (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX location_name_uindex ON location (name)").run()

	db.prepare(`
		CREATE TABLE time (
			id integer PRIMARY KEY AUTOINCREMENT,
			days text NOT NULL,
			start text NOT NULL,
			end text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX time_days_start_end_uindex ON time (days, start, end)").run()

	db.prepare(`
		CREATE TABLE description (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX description_content_uindex ON description (content)").run()
	db.prepare(`
		CREATE TABLE note (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX note_content_uindex ON note (content)").run()
	db.prepare(`
		CREATE TABLE prerequisite (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX prerequisite_content_uindex ON prerequisite (content)").run()

	db.prepare(`
		CREATE TABLE course (
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
		)
	`).run()

	db.prepare(`
		CREATE TABLE course_department (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_department_course_id_fk references course,
			department_id integer NOT NULL
				constraint course_department_department_id_fk references department
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_department_course_id_department_id_uindex ON course_department (course_id, department_id)").run()
	db.prepare(`
		CREATE TABLE course_instructor (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_instructor_course_id_fk references course,
			instructor_id integer NOT NULL
				constraint course_instructor_instructor_id_fk references instructor
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_instructor_course_id_instructor_id_uindex ON course_instructor (course_id, instructor_id)").run()
	db.prepare(`
		CREATE TABLE course_gereq (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_gereq_course_id_fk references course,
			gereq_id integer NOT NULL
				constraint course_gereq_gereq_id_fk references gereq
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_gereq_course_id_gereq_id_uindex ON course_gereq (course_id, gereq_id)").run()
	db.prepare(`
		CREATE TABLE course_location (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_location_course_id_fk references course,
			location_id integer NOT NULL
				constraint course_location_location_id_fk references location
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_location_course_id_location_id_uindex ON course_location (course_id, location_id)").run()
	db.prepare(`
		CREATE TABLE course_time (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_time_course_id_fk references course,
			time_id integer NOT NULL
				constraint course_time_time_id_fk references time
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_time_course_id_time_id_uindex ON course_time (course_id, time_id)").run()
	db.prepare(`
		CREATE TABLE course_note (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_note_course_id_fk references course,
			note_id integer NOT NULL
				constraint course_note_note_id_fk references note
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_note_course_id_note_id_uindex ON course_note (course_id, note_id)").run()
	db.prepare(`
		CREATE TABLE course_description (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_description_course_id_fk references course,
			description_id integer NOT NULL
				constraint course_description_description_id_fk references description
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_description_course_id_description_id_uindex ON course_description (course_id, description_id)").run()
	db.prepare(`
		CREATE TABLE course_prerequisite (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_prerequisite_course_id_fk references course,
			prerequisite_id integer NOT NULL
				constraint course_prerequisite_prerequisite_id_fk references prerequisite
		)
	`).run()
	db.prepare("CREATE UNIQUE INDEX course_prerequisite_course_id_prerequisite_id_uindex ON course_prerequisite (course_id, prerequisite_id)").run()
}

async function main() {
	rimraf.sync('./courses.sqlite');
	let db = new Database('./courses.sqlite');

	let begin = db.prepare('BEGIN');
	let commit = db.prepare('COMMIT');
	let rollback = db.prepare('ROLLBACK');

	// Higher order function - returns a function that always runs in a transaction
	function asTransaction(func) {
		return (
			function(...args) {
				begin.run()
				try {
					func(...args)
					commit.run()
				} finally {
					if (db.inTransaction) {
						rollback.run()
					}
				}
			}
		)()
	}

	prepareDb(db)

	let course_stmt = db.prepare(`
		INSERT INTO course (clbid, credits, crsid, level, name, number, pn, section, status, title, type, year, semester)
		VALUES (:clbid, :credits, :crsid, :level, :name, :number, :pn, :section, :status, :title, :type, :year, :semester)
	`)

	let course_note_stmt         = db.prepare("INSERT OR IGNORE INTO course_note (course_id, note_id) VALUES (?, ?)")
	let course_department_stmt   = db.prepare("INSERT OR IGNORE INTO course_department (course_id, department_id) VALUES (?, ?)")
	let course_gereq_stmt        = db.prepare("INSERT OR IGNORE INTO course_gereq (course_id, gereq_id) VALUES (?, ?)")
	let course_description_stmt  = db.prepare("INSERT OR IGNORE INTO course_description (course_id, description_id) VALUES (?, ?)")
	let course_instructor_stmt   = db.prepare("INSERT OR IGNORE INTO course_instructor (course_id, instructor_id) VALUES (?, ?)")
	let course_location_stmt     = db.prepare("INSERT OR IGNORE INTO course_location (course_id, location_id) VALUES (?, ?)")
	let course_time_stmt         = db.prepare("INSERT OR IGNORE INTO course_time (course_id, time_id) VALUES (?, ?)")
	let course_prerequisite_stmt = db.prepare("INSERT OR IGNORE INTO course_prerequisite (course_id, prerequisite_id) VALUES (?, ?)")

	let department_stmt   = db.prepare("INSERT OR IGNORE INTO department (name) VALUES (:name)")
	let gereq_stmt        = db.prepare("INSERT OR IGNORE INTO gereq (name) VALUES (:name)")
	let instructor_stmt   = db.prepare("INSERT OR IGNORE INTO instructor (name) VALUES (:name)")
	let location_stmt     = db.prepare("INSERT OR IGNORE INTO location (name) VALUES (:name)")
	let time_stmt         = db.prepare("INSERT OR IGNORE INTO time (days, start, end) VALUES (:days, :start, :end)")
	let description_stmt  = db.prepare("INSERT OR IGNORE INTO description (content) VALUES (:content)")
	let note_stmt         = db.prepare("INSERT OR IGNORE INTO note (content) VALUES (:content)")
	let prerequisite_stmt = db.prepare("INSERT OR IGNORE INTO prerequisite (content) VALUES (:content)")

	let get_department_stmt   = db.prepare("SELECT id FROM department   WHERE name = :name")
	let get_gereq_stmt        = db.prepare("SELECT id FROM gereq        WHERE name = :name")
	let get_instructor_stmt   = db.prepare("SELECT id FROM instructor   WHERE name = :name")
	let get_location_stmt     = db.prepare("SELECT id FROM location     WHERE name = :name")
	let get_time_stmt         = db.prepare("SELECT id FROM time         WHERE days = :days AND start = :start AND end = :end")
	let get_description_stmt  = db.prepare("SELECT id FROM description  WHERE content = :content")
	let get_note_stmt         = db.prepare("SELECT id FROM note         WHERE content = :content")
	let get_prerequisite_stmt = db.prepare("SELECT id FROM prerequisite WHERE content = :content")

	let dataFiles = fs.readdirSync('../data').filter(filename => filename.endsWith('.json'))

	dataFiles.forEach(filename => {
		console.log(filename)
		let data = fs.readFileSync(path.join('..', 'data', filename), 'utf-8')
		let courses = JSON.parse(data)

		asTransaction(() => {
			courses.forEach(course => {
				let {clbid, credits, crsid, level, name, number, pn=null, section=null, status, title=null, type, year, semester} = course
				let course_arg = {clbid, credits, crsid, level: String(level), name, number: String(number), pn: pn ? 1 : 0, section, status, title, type, year, semester}

				let cinfo = course_stmt.run(course_arg)

				;(course.departments || []).forEach(department => {
					department_stmt.run({name: department})
					let {id: departmentId} = get_department_stmt.get({name: department})
					course_department_stmt.run(cinfo.lastInsertROWID, departmentId)
				})

				;(course.gereqs || []).forEach(gereq => {
					gereq_stmt.run({name: gereq})
					let {id: gereqId} = get_gereq_stmt.get({name: gereq})
					course_gereq_stmt.run(cinfo.lastInsertROWID, gereqId)
				})

				;(course.instructors || []).forEach(instructor => {
					instructor_stmt.run({name: instructor})
					let {id: instructorId} = get_instructor_stmt.get({name: instructor})
					course_instructor_stmt.run(cinfo.lastInsertROWID, instructorId)
				})

				;(course.locations || []).forEach(location => {
					location_stmt.run({name: location})
					let {id: locationId} = get_location_stmt.get({name: location})
					course_location_stmt.run(cinfo.lastInsertROWID, locationId)
				})


				;(course.notes || []).forEach(note => {
					note_stmt.run({content: note})
					let {id: noteId} = get_note_stmt.get({content: note})
					course_note_stmt.run(cinfo.lastInsertROWID, noteId)
				})

				;(course.description || []).forEach(description => {
					description_stmt.run({content: description})
					let {id: descriptionId} = get_description_stmt.get({content: description})
					course_description_stmt.run(cinfo.lastInsertROWID, descriptionId)
				})


				;(course.times || []).forEach(timestring => {
					let [days, time] = timestring.split(/\s+/)
					let [start, end] = time.split('-')
					time_stmt.run({days, start, end})

					let {id: timeId} = get_time_stmt.get({days, start, end})
					course_time_stmt.run(cinfo.lastInsertROWID, timeId)
				})


				if (course.prerequisites) {
					prerequisite_stmt.run({content: course.prerequisites})
					let {id: prerequisiteId} = get_prerequisite_stmt.get({content: course.prerequisites})
					course_prerequisite_stmt.run(cinfo.lastInsertROWID, prerequisiteId)
				}
			})
		})
	})
}

main()
