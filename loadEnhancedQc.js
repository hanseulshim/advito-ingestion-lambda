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

module.exports.loadEnhancedQc = async (event) => {
	try {
		const { jobIngestionHotel } = event
		if (!jobIngestionHotel) {
			throw Error('Job ingestion not found')
		}
		const { rows } = await advito.raw(
			`select * from load_for_sourcing_dpm(${jobIngestionHotel.jobIngestionId}, ${jobIngestionHotel.clientId}, ${jobIngestionHotel.year}, ${jobIngestionHotel.month}, '${jobIngestionHotel.type}')`
		)
		if (rows.length > 0) {
			const result = await advito.raw(
				`select * from best_of_hotel_project_property(${rows[0].load_for_sourcing_dpm})`
			)
			if (result.rows.length > 0) {
				console.log(
					'result from best of logic: ',
					result.rows[0].best_of_hotel_project_property
				)
			} else {
				console.log('best of logic returned false')
			}
		}
		return true
	} catch (e) {
		console.log(e.message)
		await advito('job_ingestion_hotel')
			.update('ingestion_note', 'error')
			.where('job_ingestion_id', event.jobIngestionHotel.jobIngestionId)
		throw Error('Loading enhanced QC failed')
	}
}

module.exports.loadEnhancedQc({
	jobIngestionHotel: {
		jobIngestionId: 18862,
		clientId: 348,
		year: 2019,
		month: 'NULL',
		type: 'sourcing'
	}
})
