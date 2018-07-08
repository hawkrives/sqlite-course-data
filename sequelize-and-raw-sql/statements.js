function prepareStatements(db) {
	let course_stmt = db.prepare(`
		INSERT INTO course (
			clbid, credits, crsid, level,
			name, number, pn, section,
			status, title, type,
			year, semester,
			createdAt, updatedAt
		) VALUES (
			:clbid, :credits, :crsid, :level,
			:name, :number, :pn, :section,
			:status, :title, :type,
			:year, :semester,
			:created, :updated
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
	let course_sourcefile_stmt = db.prepare(`
		INSERT INTO course_sourcefile (course_id, sourcefile_id)
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
			course_sourcefile: course_sourcefile_stmt,
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

module.exports = prepareStatements
