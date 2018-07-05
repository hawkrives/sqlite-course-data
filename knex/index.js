const KNEX = require('knex')

const knex = KNEX({
  client: 'sqlite3',
  connection: {
    filename: './courses.sqlite',
  },
  debugging: true,
  asyncStackTraces: true,
  useNullAsDefault: true,
})

async function setup() {
  await knex.schema
    .dropTableIfExists('department')
    .createTable('department', t => {
      t.increments('id')
        .notNullable()
        .primary()

      t.text('name').notNullable()
    })

  await knex.schema.dropTableIfExists('course').createTable('course', t => {
    t.increments('id')
      .primary()
      .notNullable()

    t.integer('clbid')
      .notNullable()
      .unique()
    t.real('credits').notNullable()
    t.integer('crsid').notNullable()
    t.text('level').notNullable()
    t.text('name').notNullable()
    t.integer('number').notNullable()
    t.boolean('pn').notNullable()
    t.text('section').nullable()
    t.text('status').notNullable()
    t.text('title').nullable()
    t.text('type').notNullable()

    t.integer('year').notNullable()
    t.integer('semester').notNullable()

    t.dateTime('created_at')
      .notNullable()
      .defaultTo('CURRENT_TIMESTAMP')
    t.dateTime('updated_at')
      .notNullable()
      .defaultTo('CURRENT_TIMESTAMP')
  })

  await knex.schema
    .dropTableIfExists('course_department')
    .createTable('course_department', t => {
      t.increments('id')
        .notNullable()
        .primary()

      t.integer('course_id')
        .unsigned()
        .references('course.id')
      t.integer('department_id')
        .unsigned()
        .references('department.id')
    })

  let [d] = await knex('department').insert({ name: 'ASIAN' })

  let [c] = await knex('course').insert({
    clbid: 1,
    credits: 0,
    crsid: 1,
    level: '100',
    name: 'none',
    number: 101,
    pn: false,
    section: 'A',
    status: 'Open',
    title: 'A Course',
    type: 'Research',
    year: 2017,
    semester: 3,
  })

  await knex('course_department').insert({ course_id: c, department_id: d })

  let results = await knex('course')
    .join('course_department', 'course.id', 'course_department.course_id')
    .join('department', 'department.id', 'course_department.department_id')
    .select('course.*', 'department.name as department')
    .groupBy('course.id')

  console.log(results)
}

setup().then(knex.destroy)
