'use strict'

const Database = require('better-sqlite3')
const fs = require('fs')
const path = require('path')
const rimraf = require('rimraf')
const {performance} = require('perf_hooks')
const hasha = require('hasha')
// const {Op} = require('./better-sequelize')
const {Op} = require('sequelize')
const {init, sequelize, SourceFile, Course, Department} = require('./schema')
const prepareStatements = require('./statements')

function insertCourse({course, statements: stmt, sourceFileId}) {
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
		created: new Date().toISOString(),
		updated: new Date().toISOString(),
	})

	stmt.insert.course_sourcefile.run(cid, sourceFileId)

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

async function main() {
	let start, end
	rimraf.sync('./courses.sqlite')

	start = performance.now()
	await init()
	end = performance.now()
	console.log(`preparation: ${(end - start).toFixed(2)} ms`)

	let db = new Database('./courses.sqlite')
	let statements = prepareStatements(db)

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

	let basedir = path.join(__dirname, '..', 'data')
	let dataFiles = fs
		.readdirSync(basedir)
		.filter(filename => filename.startsWith('2') && filename.endsWith('.json'))
		.map(filename => path.join(basedir, filename))
		// .slice(0, 3)

	for (let filename of dataFiles) {
		let contents = fs.readFileSync(filename, 'utf-8')
		let dataHash = hasha(contents, {algorithm: 'sha256'})
		let file = path.basename(filename, '.json')
		let courses = JSON.parse(contents)

		let year = parseInt(file.substr(0, 4))
		let semester = parseInt(file[4])
		let [sourceFile, wasCreated] = await SourceFile.findOrCreate({
			where: {semester, year, hash: dataHash},
		})

		if (!wasCreated) {
			// if the source file already exists, we need to clean the old courses out of the database
			// TODO: figure out how to do this in one step (i.e., query by the `sourceFile`)
			let courseIds = await sourceFile
				.getCourses({attributes: ['id']})
				.map(i => i.id)
			await Course.destroy({where: {id: {[Op.in]: courseIds}}})
		}

		start = performance.now()
		asTransaction(() => {
			courses.forEach(course =>
				insertCourse({course, sourceFileId: sourceFile.id, statements}),
			)
		})
		end = performance.now()
		console.log('processed', filename, 'in', (end - start).toFixed(2).padStart(6, ' '), 'ms')
	}
}

async function load() {
	let courses = await Course.findAll({
		include: [
			{
				model: Department,
				attributes: ['name'],
				where: {name: 'ASIAN'},
			},
		],
	})

	console.log(JSON.stringify(courses, null, 2))
}

main()//.then(load)
