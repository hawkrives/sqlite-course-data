const Sequelize = require('sequelize')

const sequelize = new Sequelize('database', '', '', {
	dialect: 'sqlite',
	storage: './courses.sqlite',
	operatorsAliases: false,
	logging: false,
	benchmark: true,
})

let Department = sequelize.define(
	'department',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		name: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'department'},
)
let Instructor = sequelize.define(
	'instructor',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		name: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'instructor'},
)
let GeReq = sequelize.define(
	'gereq',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		name: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'gereq'},
)
let Location = sequelize.define(
	'location',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		name: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'location'},
)

let Time = sequelize.define(
	'time',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		days: {
			type: Sequelize.TEXT,
			allowNull: false,
			unique: 'timeCompositeIndex',
		},
		start: {
			type: Sequelize.TEXT,
			allowNull: false,
			unique: 'timeCompositeIndex',
		},
		end: {
			type: Sequelize.TEXT,
			allowNull: false,
			unique: 'timeCompositeIndex',
		},
	},
	{timestamps: false, tableName: 'time'},
)

let Description = sequelize.define(
	'description',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		content: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'description'},
)
let Note = sequelize.define(
	'note',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		content: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'note'},
)
let Prerequisite = sequelize.define(
	'prerequisite',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		content: {type: Sequelize.TEXT, allowNull: false, unique: true},
	},
	{timestamps: false, tableName: 'prerequisite'},
)

let Course = sequelize.define(
	'course',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		clbid: {type: Sequelize.INTEGER, allowNull: true, unique: true},
		credits: {type: Sequelize.REAL, allowNull: true},
		crsid: {type: Sequelize.INTEGER, allowNull: true},
		level: {type: Sequelize.TEXT, allowNull: true},
		name: {type: Sequelize.TEXT, allowNull: false},
		number: {type: Sequelize.TEXT, allowNull: true},
		pn: {type: Sequelize.BOOLEAN, allowNull: false},
		section: {type: Sequelize.TEXT, allowNull: true},
		status: {type: Sequelize.TEXT, allowNull: true},
		title: {type: Sequelize.TEXT, allowNull: true},
		type: {type: Sequelize.TEXT, allowNull: true},

		year: {type: Sequelize.INTEGER, allowNull: false},
		semester: {type: Sequelize.INTEGER, allowNull: false},
	},
	{tableName: 'course'},
)

let SourceFile = sequelize.define(
	'sourcefile',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		semester: {
			type: Sequelize.INTEGER,
			allowNull: false,
			unique: 'sourceFileCompositeIndex',
		},
		year: {
			type: Sequelize.INTEGER,
			allowNull: false,
			unique: 'sourceFileCompositeIndex',
		},
		hash: {
			type: Sequelize.TEXT,
			allowNull: false,
			unique: 'sourceFileCompositeIndex',
		},
	},
	{tableName: 'sourcefile'},
)

let CourseSourceFile = sequelize.define(
	'course_sourcefile',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_sourcefile'},
		sourcefile_id: {type: Sequelize.INTEGER, unique: 'course_sourcefile'},
	},
	{timestamps: false, tableName: 'course_sourcefile'},
)
SourceFile.belongsToMany(Course, {
	through: {model: CourseSourceFile, unique: false},
	foreignKey: 'sourcefile_id',
	constraints: false,
})
Course.belongsToMany(SourceFile, {
	through: {model: CourseSourceFile, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseDepartment = sequelize.define(
	'course_department',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_department'},
		department_id: {type: Sequelize.INTEGER, unique: 'course_department'},
	},
	{timestamps: false, tableName: 'course_department'},
)
Department.belongsToMany(Course, {
	through: {model: CourseDepartment, unique: false},
	foreignKey: 'department_id',
	constraints: false,
})
Course.belongsToMany(Department, {
	through: {model: CourseDepartment, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseGereq = sequelize.define(
	'course_gereq',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_gereq'},
		gereq_id: {type: Sequelize.INTEGER, unique: 'course_gereq'},
	},
	{timestamps: false, tableName: 'course_gereq'},
)
GeReq.belongsToMany(Course, {
	through: {model: CourseGereq, unique: false},
	foreignKey: 'gereq_id',
	constraints: false,
})
Course.belongsToMany(GeReq, {
	through: {model: CourseGereq, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseInstructor = sequelize.define(
	'course_instructor',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_instructor'},
		instructor_id: {type: Sequelize.INTEGER, unique: 'course_instructor'},
	},
	{timestamps: false, tableName: 'course_instructor'},
)
Instructor.belongsToMany(Course, {
	through: {model: CourseInstructor, unique: false},
	foreignKey: 'instructor_id',
	constraints: false,
})
Course.belongsToMany(Instructor, {
	through: {model: CourseInstructor, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseLocation = sequelize.define(
	'course_location',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER},
		location_id: {type: Sequelize.INTEGER},
	},
	{timestamps: false, tableName: 'course_location'},
)
Location.belongsToMany(Course, {
	through: {model: CourseLocation, unique: false},
	foreignKey: 'location_id',
	constraints: false,
})
Course.belongsToMany(Location, {
	through: {model: CourseLocation, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseTime = sequelize.define(
	'course_time',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER},
		time_id: {type: Sequelize.INTEGER},
	},
	{timestamps: false, tableName: 'course_time'},
)
Time.belongsToMany(Course, {
	through: {model: CourseTime, unique: false},
	foreignKey: 'time_id',
	constraints: false,
})
Course.belongsToMany(Time, {
	through: {model: CourseTime, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseDescription = sequelize.define(
	'course_description',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_description'},
		description_id: {type: Sequelize.INTEGER, unique: 'course_description'},
	},
	{timestamps: false, tableName: 'course_description'},
)
Description.belongsToMany(Course, {
	through: {model: CourseDescription, unique: false},
	foreignKey: 'description_id',
	constraints: false,
})
Course.belongsToMany(Description, {
	through: {model: CourseDescription, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CourseNote = sequelize.define(
	'course_note',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_note'},
		note_id: {type: Sequelize.INTEGER, unique: 'course_note'},
	},
	{timestamps: false, tableName: 'course_note'},
)
Note.belongsToMany(Course, {
	through: {model: CourseNote, unique: false},
	foreignKey: 'note_id',
	constraints: false,
})
Course.belongsToMany(Note, {
	through: {model: CourseNote, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

let CoursePrerequisite = sequelize.define(
	'course_prerequisite',
	{
		id: {type: Sequelize.INTEGER, primaryKey: true, autoIncrement: true},
		course_id: {type: Sequelize.INTEGER, unique: 'course_prerequisite'},
		prerequisite_id: {type: Sequelize.INTEGER, unique: 'course_prerequisite'},
	},
	{timestamps: false, tableName: 'course_prerequisite'},
)
Prerequisite.belongsToMany(Course, {
	through: {model: CoursePrerequisite, unique: false},
	foreignKey: 'prerequisite_id',
	constraints: false,
})
Course.belongsToMany(Prerequisite, {
	through: {model: CoursePrerequisite, unique: false},
	foreignKey: 'course_id',
	constraints: false,
})

async function init() {
	await sequelize.sync({force: true})

	// await sequelize.query(`DROP TABLE IF EXISTS description_fts;`)
	// await sequelize.query(
	// 	`CREATE VIRTUAL TABLE IF NOT EXISTS description_fts USING fts3(id, content);`,
	// )
}

module.exports = {
	sequelize,
	init,
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
}
