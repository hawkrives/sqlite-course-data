const Sequelize = require('sequelize');

async function main() {
	const sequelize = new Sequelize('database', '', '', {
		dialect: 'sqlite',
		storage: './database.sqlite',
		operatorsAliases: false,
	})

	const User = sequelize.define('user', {
		firstName: Sequelize.TEXT,
		lastName: Sequelize.TEXT,
	});

	// force: true will drop the table if it already exists
	await User.sync({force: true})

	await User.create({firstName: 'John', lastName: 'Hancock'});
}

main()
