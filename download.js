const fs = require('fs')
const got = require('got')
const mkdir = require('make-dir')

async function main() {
	let {body: info} = await got.get('https://StoDevX.github.io/course-data/info.json', {json: true})

	let files = info.files.filter(file => file.type === 'json').filter(file => file.year >= 2013)

	await mkdir('./data')
	let promises = files.map(file =>
		got.get(`https://StoDevX.github.io/course-data/${file.path}`).then(data => {
			let path = file.path.replace('terms', 'data')
			console.log(path)
			fs.writeFileSync(`${__dirname}/${path}`, data.body, 'utf-8')
		})
	)

	await Promise.all(promises)
}

main()
