require('array.prototype.flat/auto')
require('array.prototype.flatmap/auto')

const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./courses.sqlite');

const {promisify: pify} = require('util')

const data = require('../data/20131.json');

db.serialize(() => {
	db.exec(`
		CREATE TABLE department (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		);

		CREATE TABLE instructor (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		);

		CREATE TABLE gereq (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		);

		CREATE TABLE location (
			id integer PRIMARY KEY AUTOINCREMENT,
			name text NOT NULL
		);

		CREATE TABLE time (
			id integer PRIMARY KEY AUTOINCREMENT,
			days text NOT NULL,
			start text NOT NULL,
			end text NOT NULL
		);

		CREATE TABLE description (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		);

		CREATE TABLE note (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		);

		CREATE TABLE prerequisite (
			id integer PRIMARY KEY AUTOINCREMENT,
			content text NOT NULL
		);
	`)

	db.exec(`
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
		);
	`)

	db.exec(`
		CREATE TABLE course_department (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_department_course_id_fk references course,
			department_id integer NOT NULL
				constraint course_department_department_id_fk references department
		);

		CREATE TABLE course_instructor (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_instructor_course_id_fk references course,
			instructor_id integer NOT NULL
				constraint course_instructor_instructor_id_fk references instructor
		);

		CREATE TABLE course_gereq (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_gereq_course_id_fk references course,
			gereq_id integer NOT NULL
				constraint course_gereq_gereq_id_fk references gereq
		);

		CREATE TABLE course_location (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_location_course_id_fk references course,
			location_id integer NOT NULL
				constraint course_location_location_id_fk references location
		);

		CREATE TABLE course_time (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_time_course_id_fk references course,
			time_id integer NOT NULL
				constraint course_time_time_id_fk references time
		);

		CREATE TABLE course_note (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_note_course_id_fk references course,
			note_id integer NOT NULL
				constraint course_note_note_id_fk references note
		);

		CREATE TABLE course_description (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_description_course_id_fk references course,
			description_id integer NOT NULL
				constraint course_description_description_id_fk references description
		);

		CREATE TABLE course_prerequisite (
			id integer PRIMARY KEY AUTOINCREMENT,
			course_id integer NOT NULL
				constraint course_prerequisite_course_id_fk references course,
			prerequisite_id integer NOT NULL
				constraint course_prerequisite_prerequisite_id_fk references prerequisite
		);
	`);
})

db.serialize(() => {
	let dept_stmt = db.prepare("INSERT INTO department (name) VALUES (?)");
	let gereq_stmt = db.prepare("INSERT INTO gereq (name) VALUES (?)");
	let instructor_stmt = db.prepare("INSERT INTO instructor (name) VALUES (?)");
	let location_stmt = db.prepare("INSERT INTO location (name) VALUES (?)");

	let time_stmt = db.prepare("INSERT INTO time (days, start, end) VALUES (?, ?, ?)");

	let description_stmt = db.prepare("INSERT INTO description (content) VALUES (?)");
	let note_stmt = db.prepare("INSERT INTO note (content) VALUES (?)");
	let prerequisite_stmt = db.prepare("INSERT INTO prerequisite (content) VALUES (?)");

	//

	let departments = [...new Set(data.flatMap(c => c.departments))].filter(Boolean)
	let gereqs = [...new Set(data.flatMap(c => c.gereqs))].filter(Boolean)
	let instructors = [...new Set(data.flatMap(c => c.instructors))].filter(Boolean)
	let locations = [...new Set(data.flatMap(c => c.locations))].filter(Boolean)

	let times = [...new Set(data.flatMap(c => c.times))].filter(Boolean)

	let descriptions = [...new Set(data.flatMap(c => c.description))].filter(Boolean)
	let notes = [...new Set(data.flatMap(c => c.notes))].filter(Boolean)
	let prerequisites = [...new Set(data.map(c => c.prerequisites))].filter(Boolean)

	// console.log(descriptions)

	//

	departments.forEach(dept => dept_stmt.run(dept));
	dept_stmt.finalize();
	console.log('done with departments')

	gereqs.forEach(gereq => gereq_stmt.run(gereq));
	gereq_stmt.finalize();
	console.log('done with gereqs')

	instructors.forEach(instructor => instructor_stmt.run(instructor));
	instructor_stmt.finalize();
	console.log('done with instructors')

	locations.forEach(location => location_stmt.run(location));
	location_stmt.finalize();
	console.log('done with locations')

	times.forEach(time => {
		let [days, times] = time.split(/\s+/)
		let [start, end] = times.split('-')
		time_stmt.run([days, start, end])
	});
	time_stmt.finalize();
	console.log('done with times')

	descriptions.forEach(description => description_stmt.run(description));
	description_stmt.finalize();
	console.log('done with descriptions')

	notes.forEach(note => note_stmt.run(note));
	note_stmt.finalize();
	console.log('done with notes')

	prerequisites.forEach(prerequisite => prerequisite_stmt.run(prerequisite));
	prerequisite_stmt.finalize();
	console.log('done with prerequisites')
})

db.serialize(() => {
	let departments = new Map()

	db.each("SELECT * FROM department", (err, row) => departments.set(row.id, row));

	console.log(departments)
	// data.forEach(course => {
	// 	(course.departments || []).forEach(dept => {
	// 		console.log(dept);
	// 		dept_stmt.run(dept);
	// 	})
	// })

	//

// 	let stmt = db.prepare("INSERT INTO lorem VALUES (?)");
// 	for (let i = 0; i < 10; i++) {
// 		stmt.run("Ipsum " + i);
// 	}
// 	stmt.finalize();
//
// 	db.each("SELECT rowid AS id, info FROM lorem", (err, row) => {
// 		console.log(row.id + ": " + row.info);
// 	});
});

// db.each("SELECT id, name FROM department", (err, row) => {
// 	console.log(row.id + ": " + row.name);
// });

db.close();
