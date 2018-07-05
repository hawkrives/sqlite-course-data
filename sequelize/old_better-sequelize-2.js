'use strict'

const { performance } = require('perf_hooks')

// const Database = require('better-sqlite3')
require('array.prototype.flatmap/auto')
const fs = require('fs')
const path = require('path')
const pMap = require('p-map')
const pSeries = require('p-each-series')
const hasha = require('hasha')
const { Op } = require('sequelize')
const {
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
} = require('./better-sequelize-schema')

async function precreateExtractedDataTypes(sequelize, courses, cache) {
  let types = [
    {
      dbType: Department,
      extract: c => c.departments,
      makeWhereClause: name => ({ name }),
    },
    {
      dbType: GeReq,
      extract: c => c.gereqs,
      makeWhereClause: name => ({ name }),
    },
    {
      dbType: Instructor,
      extract: c => c.instructors,
      makeWhereClause: name => ({ name }),
    },
    {
      dbType: Location,
      extract: c => c.locations,
      makeWhereClause: name => ({ name }),
    },
    {
      dbType: Time,
      extract: c => c.times,
      makeWhereClause: timestring => {
        let [days, timestr] = timestring.split(/\s+/)
        let [start, end] = timestr.split('-')
        return { days, start, end }
      },
    },
    {
      dbType: Note,
      extract: c => c.notes,
      makeWhereClause: content => ({ content }),
    },
    {
      dbType: Description,
      extract: c => c.description,
      makeWhereClause: content => ({ content }),
    },
    {
      dbType: Prerequisite,
      extract: c => c.prerequisites,
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
      console.log('extract:', (_end - _start).toFixed(2), 'ms')

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
      console.log('lookup:', (_end - _start).toFixed(2), 'ms')

      found = new Set(found)
      let remaining = collected.filter(item => !found.has(item))

      _start = performance.now()
      await dbType.bulkCreate([...remaining].map(makeWhereClause), {
        transaction: t,
        validate: true,
      })
      _end = performance.now()
      console.log('insert:', (_end - _start).toFixed(2), 'ms')
    })

    let end = Date.now()
    console.log(`${dbType.name}: ${end - start}ms`)
    console.log()
  }
  console.log()
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

/*

I was able to insert bulk associations in a single separate query by creating
a separate model for the N:M association. So I have my PostTags model and then
I have PostId and TagId and can do

```
PostTags.bulkCreate([{postId: 1, tagId: 1}, {postId: 2, tagId: 1}])
```

*/

async function insertCourses(courses, sourceFile, cachedItems) {
  let start, end
  // bulkCreate all courses,
  // then bulkCreate the individual relationships by type

  // massage the courses into insertable format
  start = performance.now()
  let coursesToInsert = courses.map(buildCourse)
  let clbids = courses.map(c => c.clbid)
  end = performance.now()
  console.log('c:preparation', (end - start).toFixed(2), 'ms')

  // insert the courses
  start = performance.now()
  await sequelize.transaction(async t => {
    await Course.bulkCreate(coursesToInsert, { transaction: t, validate: true })
  })
  end = performance.now()
  console.log('c:course:insert', (end - start).toFixed(2), 'ms')

  // retrieve the courses from the database
  start = performance.now()
  let insertedCourses = await Course.findAll({where: {clbid: {[Op.in]: clbids}}, fields: ['id']})
  let courseIds = insertedCourses.map(c => c.id)
  end = performance.now()
  console.log('c:course:retrieval', (end - start).toFixed(2), 'ms')

  // assign to the appropriate SourceFile
  start = performance.now()
  await sequelize.transaction(async t => {
    await CourseSourceFile.bulkCreate(
      [...courseIds.keys()].map(cid => ({course_id: cid, sourcefile_id: sourceFile.id})),
      { transaction: t, validate: true }
    )
  })
  end = performance.now()
  console.log('c:course:sourcefile', (end - start).toFixed(2), 'ms')

  /////

  // for (let {type, key} of [{type: Department, key: 'departments'}]) {
  //   cachedItems.get(type).get()
  // }

  /////

//   let prepared = buildCourse(courseData)
//
//   let alreadyExists = await Course.count({ where: { clbid }, transaction })
//   if (alreadyExists) {
//     return null
//   }
//
//   let course = await Course.create(prepared, { transaction })
//
//   let splitTimes = (courseData.times || []).map(timestring => {
//     let [days, timestr] = timestring.split(/\s+/)
//     let [start, end] = timestr.split('-')
//     return { days, start, end }
//   })
//
//   let [
//     departments,
//     gereqs,
//     instructors,
//     locations,
//     notes,
//     descriptions,
//     times,
//     prerequisites,
//   ] = await Promise.all([
//     Department.findAll({
//       where: { name: { [Op.in]: courseData.departments || [] } },
//       transaction,
//     }),
//     GeReq.findAll({
//       where: { name: { [Op.in]: courseData.gereqs || [] } },
//       transaction,
//     }),
//     Instructor.findAll({
//       where: { name: { [Op.in]: courseData.instructors || [] } },
//       transaction,
//     }),
//     Location.findAll({
//       where: { name: { [Op.in]: courseData.locations || [] } },
//       transaction,
//     }),
//     Note.findAll({
//       where: { content: { [Op.in]: courseData.notes || [] } },
//       transaction,
//     }),
//     Description.findAll({
//       where: { content: { [Op.in]: courseData.description || [] } },
//       transaction,
//     }),
//     Time.findAll({
//       where: {
//         [Op.or]: splitTimes.map(({ days, start, end }) => ({
//           days,
//           start,
//           end,
//         })),
//       },
//       transaction,
//     }),
//     Prerequisite.findOne({
//       where: { content: courseData.prerequisites },
//       transaction,
//     }),
//   ])
//
//   return Promise.all([
//     course.setDepartments(departments, { transaction }),
//     course.setGereqs(gereqs, { transaction }),
//     course.setInstructors(instructors, { transaction }),
//     course.setLocations(locations, { transaction }),
//     course.setNotes(notes, { transaction }),
//     course.setDescriptions(descriptions, { transaction }),
//     course.setTimes(times, { transaction }),
//     course.setPrerequisites(prerequisites, { transaction }),
//     course.setSourcefiles(sourceFile, { transaction }),
//   ])
}

async function main() {
  let start, end

  start = Date.now()
  await sequelize.sync({ force: true })
  end = Date.now()
  console.log(`preparation: ${end - start} ms`)

  let basedir = path.join(__dirname, 'data')
  let dataFiles = fs
    .readdirSync(basedir)
    .filter(filename => filename.startsWith('2') && filename.endsWith('.json'))
    .map(filename => path.join(basedir, filename))

  let cache = new Map()

  for (let filename of dataFiles .slice(0, 3)) {
    let contents = fs.readFileSync(filename, 'utf-8')
    let dataHash = hasha(contents, { algorithm: 'sha256' })
    let file = path.basename(filename, '.json')
    console.log('file:', file, 'hash:', dataHash)
    let courses = JSON.parse(contents)

    start = Date.now()
    await precreateExtractedDataTypes(sequelize, courses, cache)
    end = Date.now()
    console.log(`extraction: ${end - start} ms`)
    console.log()

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
    await insertCourses(courses, sourceFile)
    end = Date.now()
    console.log(`courses: ${end - start} ms`)

    console.log(await sourceFile.countCourses())

    // break
  }
}

async function load() {
  // let courses = await Course.findAll({ include: [{ all: true }]})
  // courses = courses.map(c => c.dataValues)
  // console.log(courses)
  // let sourceFile = await SourceFile.find({where: {semester: 1, year: 2013, hash: '87823eb624b4ed8c4b781040ee77d014e72ced66982a786bf36bb1f630056d6c'}, attributes: ['id']})

  // let courseIds = await sourceFile.getCourses({attributes: ['id']}).map(i => i.id)

  // let intermediate = await CourseSourceFile.findAll({where: {sourcefile_id: sourceFile.id}})

  // console.log(intermediate)

  // let matches = await Course.destroy({where: {id: {[Op.in]: courseIds}}})
  // for (let key in matches) {
  //   console.log(key)
  // }
  // console.log(matches)

  Course.find
}

main() //.then(load)

// load()
