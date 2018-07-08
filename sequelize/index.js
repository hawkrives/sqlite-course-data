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
	DescriptionFts,
} = require('./schema')

async function precreateExtractedDataTypes(sequelize, courses) {
	await Promise.all([
		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
			let extract = c => c.times
			let dbType = Time
			let makeWhereClause = timestring => {
				let [days, timestr] = timestring.split(/\s+/)
				let [start, end] = timestr.split('-')
				return {days, start, end}
			}

			let collected = [
				...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
			]

			// ensure times are normalized
			collected = collected
				.map(makeWhereClause)
				.map(item => `${item.days} ${item.start}-${item.end}`)

			let found = await dbType
				.findAll({
					where: {[Op.or]: items},
					transaction: t,
					raw: true,
				})
				.map(item => `${item.days} ${item.start}-${item.end}`)

			found = new Set(found)
			let remaining = collected.filter(item => !found.has(item))

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),

		sequelize.transaction(async t => {
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

			await dbType.bulkCreate(remaining.map(makeWhereClause), {
				transaction: t,
				raw: true,
			})
		}),
	])
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

async function loadCourse({course: courseData, transaction, sourceFile}) {
	let courseInfo = buildCourse(courseData)
	let course = await Course.create(courseInfo, {transaction, raw: true})

	let splitTimes = (courseData.times || []).map(timestring => {
		let [days, timestr] = timestring.split(/\s+/)
		let [start, end] = timestr.split('-')
		return {days, start, end}
	})

	let start, end

	start = performance.now()
	let [
		departments,
		gereqs,
		instructors,
		locations,
		notes,
		descriptions,
		times,
		prerequisites,
	] = await Promise.all([
		Department.findAll({
			where: {name: {[Op.in]: courseData.departments || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		GeReq.findAll({
			where: {name: {[Op.in]: courseData.gereqs || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		Instructor.findAll({
			where: {name: {[Op.in]: courseData.instructors || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		Location.findAll({
			where: {name: {[Op.in]: courseData.locations || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		Note.findAll({
			where: {content: {[Op.in]: courseData.notes || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		Description.findAll({
			where: {content: {[Op.in]: courseData.description || []}},
			transaction,
			raw: true,
		}).map(x => x.id),
		Time.findAll({
			where: {
				[Op.or]: splitTimes.map(({days, start, end}) => ({
					days,
					start,
					end,
				})),
			},
			transaction,
			raw: true,
		}).map(x => x.id),
		Prerequisite.findOne({
			where: {content: courseData.prerequisites},
			transaction,
			raw: true,
		}),
	])
	end = performance.now()
	console.log('read', end - start, 'ms')

	start = performance.now()
	await Promise.all([
		course.setDepartments(departments, {transaction}),
		course.setGereqs(gereqs, {transaction}),
		course.setInstructors(instructors, {transaction}),
		course.setLocations(locations, {transaction}),
		course.setNotes(notes, {transaction}),
		course.setDescriptions(descriptions, {transaction}),
		course.setTimes(times, {transaction}),
		prerequisites && course.setPrerequisites(prerequisites.id, {transaction}),
		course.setSourcefiles(sourceFile, {transaction}),
	])
	end = performance.now()
	console.log('write', end - start, 'ms')
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
		.slice(0, 1)

	for (let filename of dataFiles) {
		let contents = fs.readFileSync(filename, 'utf-8')
		let dataHash = hasha(contents, {algorithm: 'sha256'})
		let file = path.basename(filename, '.json')
		console.log('file:', file, 'hash:', dataHash)
		let courses = JSON.parse(contents)

		start = Date.now()
		await precreateExtractedDataTypes(sequelize, courses)
		end = Date.now()
		console.log(`extraction: ${end - start} ms`)
		// console.log()
		// process.exit(0)

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

		start = Date.now()
		await sequelize.transaction(async t => {
			await Course.bulkCreate(courses.map(buildCourse), {
				transaction: t,
				raw: true,
			})
		})
		end = Date.now()
		console.log(`courses: ${end - start} ms`)

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
