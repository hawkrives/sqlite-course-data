'use strict'

const {performance} = require('perf_hooks')

require('array.prototype.flatmap/auto')
const fs = require('fs')
const path = require('path')
const pMap = require('p-map')
const pSeries = require('p-each-series')
const hasha = require('hasha')
// const { Op } = require('../sequelize-and-raw-sql/sequelize')
const {Op} = require('sequelize')
const fromPairs = require('lodash/fromPairs')
const {
	init,
	sequelize,
	Department,
	Instructor,
	GeReq,
	Location,
	Time,
	Description,
	Note,
	Prerequisite,
	SourceFile,
	Course,
	CourseSourceFile,
	CourseDepartment,
	CourseGereq,
	CourseInstructor,
	CourseLocation,
	CourseTime,
	CourseDescription,
	CourseNote,
	CoursePrerequisite,
} = require('./schema')

async function precreateExtractedDataTypes(sequelize, courses, cache) {
	let departments = async t => {
		let extract = c => c.departments
		let dbType = Department
		let makeWhereClause = name => ({name})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {name: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.name)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['departments'] = fromPairs(results.map(row => [row.name, row.id]))
	}

	let gereqs = async t => {
		let extract = c => c.gereqs
		let dbType = GeReq
		let makeWhereClause = name => ({name})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {name: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.name)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['gereqs'] = fromPairs(results.map(row => [row.name, row.id]))
	}

	let instructors = async t => {
		let extract = c => c.instructors
		let dbType = Instructor
		let makeWhereClause = name => ({name})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {name: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.name)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['instructors'] = fromPairs(results.map(row => [row.name, row.id]))
	}

	let locations = async t => {
		let extract = c => c.locations
		let dbType = Location
		let makeWhereClause = name => ({name})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {name: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.name)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['locations'] = fromPairs(results.map(row => [row.name, row.id]))
	}

	let times = async t => {
		let extract = c => c.times
		let dbType = Time
		let makeWhereClause = timestring => {
			let [days, timestr] = timestring.split(/\s+/)
			let [start, end] = timestr.split('-')
			return {days, start, end}
		}
		let strTime = ({days, start, end}) => `${days} ${start}-${end}`

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		collected = collected.map(makeWhereClause)

		let found = await dbType
			.findAll({
				where: {[Op.or]: collected},
				transaction: t,
				raw: true,
			})
			.map(strTime)

		found = new Set(found)
		let remaining = collected
			// ensure times are normalized
			.map(strTime)
			.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['times'] = fromPairs(results.map(row => [strTime(row), row.id]))
	}

	let notes = async t => {
		let extract = c => c.notes
		let dbType = Note
		let makeWhereClause = content => ({content})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {content: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.content)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['notes'] = fromPairs(results.map(row => [row.content, row.id]))
	}

	let description = async t => {
		let extract = c => c.description
		let dbType = Description
		let makeWhereClause = content => ({content})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {content: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.content)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['description'] = fromPairs(results.map(row => [row.content, row.id]))
	}

	let prerequisites = async t => {
		let extract = c => c.prerequisites
		let dbType = Prerequisite
		let makeWhereClause = content => ({content})

		let collected = [
			...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
		]

		let found = await dbType
			.findAll({
				where: {content: {[Op.in]: collected}},
				transaction: t,
				raw: true,
			})
			.map(item => item.content)

		found = new Set(found)
		let remaining = collected.filter(item => !found.has(item))

		let results = await dbType.bulkCreate(remaining.map(makeWhereClause), {
			transaction: t,
			raw: true,
		})

		cache['prerequisite'] = fromPairs(results.map(row => [row.content, row.id]))
	}

	await sequelize.transaction(t =>
		Promise.all([
			departments(t),
			gereqs(t),
			instructors(t),
			locations(t),
			times(t),
			notes(t),
			description(t),
			prerequisites(t),
		]),
	)

	return cache
}

function buildCourse(course) {
	return {
		clbid: course.clbid,
		credits: course.credits,
		crsid: course.crsid,
		level: String(course.level),
		name: course.name,
		number: String(course.number),
		pn: course.pn ? 1 : 0,
		section: course.section,
		status: course.status,
		title: course.title,
		type: course.type,
		year: course.year,
		semester: course.semester,
	}
}

async function fleshOutCourses({
	courses,
	courseIds,
	transaction,
	cache,
	sourceFile,
}) {
	courses = courses.map((c, i) => ({...c, rowid: courseIds[i]}))

	// prettier-ignore
	let course_sourcefile = courses.flatMap(c => ({course_id: c.rowid, sourcefile_id: sourceFile.id, }))

	// prettier-ignore
	let course_department = courses
		.filter(c => c.departments)
		.flatMap(c => c.departments.map(name => ({course_id: c.rowid, department_id: cache.departments[name], })))

	// prettier-ignore
	let course_instructor = courses
		.filter(c => c.instructors)
		.flatMap(c => c.instructors.map(name => ({course_id: c.rowid, instructor_id: cache.instructors[name], })))

	// prettier-ignore
	let course_gereq = courses
		.filter(c => c.gereqs)
		.flatMap(c => c.gereqs.map(name => ({course_id: c.rowid, gereq_id: cache.gereqs[name], })), )

	// prettier-ignore
	let course_location = courses
		.filter(c => c.locations)
		.flatMap(c => c.locations.map(name => ({course_id: c.rowid, location_id: cache.locations[name], })), )

	// prettier-ignore
	let course_time = courses
		.filter(c => c.times)
		.flatMap(c => c.times.map(timestr => ({course_id: c.rowid, time_id: cache.times[timestr], })), )

	// prettier-ignore
	let course_note = courses
		.filter(c => c.notes)
		.flatMap(c => c.notes.map(name => ({course_id: c.rowid, note_id: cache.notes[name], })), )

	// prettier-ignore
	let course_description = courses
		.filter(c => c.description)
		.flatMap(c => c.description.map(name => ({course_id: c.rowid, description_id: cache.description[name], })), )

	// prettier-ignore
	let course_prerequisite = courses
		.filter(c => c.prerequisites)
		.flatMap(c => ({course_id: c.rowid, prerequisite_id: cache.prerequisite[c.prerequisites], }))

	await Promise.all([
		CourseSourceFile.bulkCreate(course_sourcefile, {transaction}),
		CourseDepartment.bulkCreate(course_department, {transaction}),
		CourseGereq.bulkCreate(course_instructor, {transaction}),
		CourseInstructor.bulkCreate(course_gereq, {transaction}),
		CourseLocation.bulkCreate(course_location, {transaction}),
		CourseTime.bulkCreate(course_time, {transaction}),
		CourseDescription.bulkCreate(course_note, {transaction}),
		CourseNote.bulkCreate(course_description, {transaction}),
		CoursePrerequisite.bulkCreate(course_prerequisite, {transaction}),
	])
}

async function main() {
	let start, end

	start = Date.now()
	await init()
	end = Date.now()
	console.log(`preparation: ${end - start} ms`)

	let basedir = path.join(__dirname, '..', 'data')
	let dataFiles = fs
		.readdirSync(basedir)
		.filter(filename => filename.startsWith('2') && filename.endsWith('.json'))
		.map(filename => path.join(basedir, filename))
		//.slice(0, 3)

	let dataCache = Object.create(null)

	for (let filename of dataFiles) {
		let contents = fs.readFileSync(filename, 'utf-8')
		let dataHash = hasha(contents, {algorithm: 'sha256'})
		let file = path.basename(filename, '.json')
		// console.log('file:', file, 'hash:', dataHash)
		let courses = JSON.parse(contents)

		start = performance.now()
		await precreateExtractedDataTypes(sequelize, courses, dataCache)
		// end = performance.now()
		// console.log(`extraction: ${end - start} ms`)

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

		// start = performance.now()
		await sequelize.transaction(async t => {
			let results = await Course.bulkCreate(courses.map(buildCourse), {
				transaction: t,
				raw: true,
			})

			let ids = results.map(row => row.id)

			await fleshOutCourses({
				courses,
				courseIds: ids,
				transaction: t,
				cache: dataCache,
				sourceFile,
			})
		})
		end = performance.now()
		console.log('processed', filename, 'in', (end - start).toFixed(2).padStart(6, ' '), 'ms')

		// console.log(await sourceFile.countCourses())

		// break
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
	// console.log(JSON.stringify(courses, null, 2))
}

main() //.then(load)

// load()
