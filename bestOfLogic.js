'use strict'

const yenv = require('yenv')
const env = yenv('./env.yml', {
	env: 'default'
})
const advito = require('knex')({
	client: 'pg',
	connection: {
		user: process.env.DB_USER ? process.env.DB_USER : env.DB_USER,
		host: process.env.DB_HOST ? process.env.DB_HOST : env.DB_HOST,
		database: process.env.DB_ADVITO ? process.env.DB_ADVITO : env.DB_ADVITO,
		password: process.env.DB_PASSWORD
			? process.env.DB_PASSWORD
			: env.DB_PASSWORD
	}
})

module.exports.bestOfLogic = async (event) => {
	try {
		const { hotelProjectId } = event
		if (!hotelProjectId) {
			throw Error('hotel project not found')
		}
		console.log('Running for hotel project id: ', hotelProjectId)
		const result = await advito.raw(
			`select * from best_of_hotel_project_property(${hotelProjectId})`
		)
		if (result.rows.length > 0) {
			console.log(
				'result from best of logic: ',
				result.rows[0].best_of_hotel_project_property
			)
		} else {
			console.log('best of logic returned false')
		}
		await advito.destroy()
		return true
	} catch (e) {
		console.log(e.message)
		throw e
	}
}

// module.exports.bestOfLogic({
// 	hotelProjectId: 73
// })
