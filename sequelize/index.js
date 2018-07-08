'use strict'

const { performance } = require('perf_hooks')

// const Database = require('better-sqlite3')
require('array.prototype.flatmap/auto')
const fs = require('fs')
const path = require('path')
const pMap = require('p-map')
const pSeries = require('p-each-series')
const hasha = require('hasha')
// const { Op } = require('../sequelize-and-raw-sql/sequelize')
const { Op } = require('sequelize')
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
  let types = [
    {
      extract: c => c.departments,
      dbType: Department,
      makeWhereClause: name => ({ name }),
    },
    {
      extract: c => c.gereqs,
      dbType: GeReq,
      makeWhereClause: name => ({ name }),
    },
    {
      extract: c => c.instructors,
      dbType: Instructor,
      makeWhereClause: name => ({ name }),
    },
    {
      extract: c => c.locations,
      dbType: Location,
      makeWhereClause: name => ({ name }),
    },
    {
      extract: c => c.times,
      dbType: Time,
      makeWhereClause: timestring => {
        let [days, timestr] = timestring.split(/\s+/)
        let [start, end] = timestr.split('-')
        return { days, start, end }
      },
    },
    {
      extract: c => c.notes,
      dbType: Note,
      makeWhereClause: content => ({ content }),
    },
    {
      extract: c => c.description,
      dbType: Description,
      makeWhereClause: content => ({ content }),
    },
    {
      extract: c => c.prerequisites,
      dbType: Prerequisite,
      makeWhereClause: content => ({ content }),
    },
  ]

  for (let { extract, makeWhereClause, dbType } of types) {
    let start = Date.now()

    await sequelize.transaction(async t => {
      let _start, _end

      _start = performance.now()
      let collected = [
        ...new Set(courses.flatMap(extract).filter(item => Boolean(item))),
      ]

      if (dbType === Time) {
        // ensure times are normalized
        collected = collected
          .map(makeWhereClause)
          .map(item => `${item.days} ${item.start}-${item.end}`)
      }
      _end = performance.now()
      // console.log('extract:', (_end - _start).toFixed(2), 'ms')

      _start = performance.now()
      let found = []
      switch (dbType) {
        case Department:
        case GeReq:
        case Instructor:
        case Location: {
          found = await dbType.findAll({
            where: { name: { [Op.in]: collected } },
            transaction: t,
          })
          found = found.map(item => item.name)
          break
        }
        case Time: {
          let items = collected.map(makeWhereClause)
          found = await dbType.findAll({
            where: { [Op.or]: items },
            transaction: t,
          })
          found = found.map(item => `${item.days} ${item.start}-${item.end}`)
          break
        }
        case Note:
        case Description:
        case Prerequisite: {
          found = await dbType.findAll({
            where: { content: { [Op.in]: collected } },
            transaction: t,
          })
          found = found.map(item => item.content)
          break
        }
        default: {
          throw new Error('unknown type')
        }
      }
      _end = performance.now()
      // console.log('lookup:', (_end - _start).toFixed(2), 'ms')

      found = new Set(found)
      let remaining = collected.filter(item => !found.has(item))

      _start = performance.now()
      await dbType.bulkCreate([...remaining].map(makeWhereClause), {
        transaction: t,
        validate: true,
      })
      // if (dbType === Description) {
      //   await DescriptionFts.bulkCreate([...remaining].map(text => ({content: text})), {transaction: t})
      // }
      _end = performance.now()
      // console.log('insert:', (_end - _start).toFixed(2), 'ms')
    })

    let end = Date.now()
    // console.log(`${dbType.name}: ${end - start}ms`)
    // console.log()
  }
  // console.log()
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

async function loadCourse({ course: courseData, transaction, sourceFile }) {
  let courseInfo = buildCourse(courseData)
  let course = await Course.create(courseInfo, { transaction })

  // let splitTimes = (courseData.times || []).map(timestring => {
  //   let [days, timestr] = timestring.split(/\s+/)
  //   let [start, end] = timestr.split('-')
  //   return { days, start, end }
  // })

  // let start, end

  // start = performance.now()
  // let [
  //   departments,
  //   gereqs,
  //   instructors,
  //   locations,
  //   notes,
  //   descriptions,
  //   times,
  //   prerequisites,
  // ] = await Promise.all([
  //   Department.findAll({
  //     where: { name: { [Op.in]: courseData.departments || [] } },
  //     transaction,
  //   }),
  //   GeReq.findAll({
  //     where: { name: { [Op.in]: courseData.gereqs || [] } },
  //     transaction,
  //   }),
  //   Instructor.findAll({
  //     where: { name: { [Op.in]: courseData.instructors || [] } },
  //     transaction,
  //   }),
  //   Location.findAll({
  //     where: { name: { [Op.in]: courseData.locations || [] } },
  //     transaction,
  //   }),
  //   Note.findAll({
  //     where: { content: { [Op.in]: courseData.notes || [] } },
  //     transaction,
  //   }),
  //   Description.findAll({
  //     where: { content: { [Op.in]: courseData.description || [] } },
  //     transaction,
  //   }),
  //   Time.findAll({
  //     where: {
  //       [Op.or]: splitTimes.map(({ days, start, end }) => ({
  //         days,
  //         start,
  //         end,
  //       })),
  //     },
  //     transaction,
  //   }),
  //   Prerequisite.findOne({
  //     where: { content: courseData.prerequisites },
  //     transaction,
  //   }),
  // ])
  // end = performance.now()
  // console.log('read', end - start, 'ms')

  // start = performance.now()
  // await Promise.all([
  //   course.setDepartments(departments, { transaction }),
  //   course.setGereqs(gereqs, { transaction }),
  //   course.setInstructors(instructors, { transaction }),
  //   course.setLocations(locations, { transaction }),
  //   course.setNotes(notes, { transaction }),
  //   course.setDescriptions(descriptions, { transaction }),
  //   course.setTimes(times, { transaction }),
  //   course.setPrerequisites(prerequisites, { transaction }),
  //   course.setSourcefiles(sourceFile, { transaction }),
  // ])
  // end = performance.now()
  // console.log('write', end - start, 'ms')
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

  for (let filename of dataFiles/* .slice(0, 3) */) {
    let contents = fs.readFileSync(filename, 'utf-8')
    let dataHash = hasha(contents, { algorithm: 'sha256' })
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
      where: { semester, year, hash: dataHash },
    })

    if (!wasCreated) {
      // if the source file already exists, we need to clean the old courses out of the database
      // TODO: figure out how to do this in one step (i.e., query by the `sourceFile`)
      let courseIds = await sourceFile
        .getCourses({ attributes: ['id'] })
        .map(i => i.id)
      await Course.destroy({ where: { id: { [Op.in]: courseIds } } })
    }

    start = Date.now()
    await sequelize.transaction(async t => {
      for (let course of courses) {
        await loadCourse({ course, transaction: t, sourceFile })
      }
    })
    end = Date.now()
    console.log(`courses: ${end - start} ms`)

    // console.log(await sourceFile.countCourses())

    // break
  }
}

async function load() {
  let courses = await Course.findAll(
    {
      include: [{
        model: Department,
        attributes: ['name'],
        where: { name: 'ASIAN' },
      }],
    },
  )
  // console.log(JSON.stringify(courses, null, 2))
}

main() //.then(load)

// load()

// CREATE VIRTUAL TABLE <TABLE> USING fts3(<COLUMN NAME> TEXT);

// SELECT * FROM <TABLE NAME> WHERE <COLUMN NAME> MATCH <INPUT>;
