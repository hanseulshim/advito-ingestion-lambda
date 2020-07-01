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

module.exports.approveFile = async (event) => {
	try {
		const { jobIngestionId, clientId, type } = event
		if (!jobIngestionId) {
			throw Error('Job ingestion not found')
		}
		await advito.raw(
			`select * from approve_for_sourcing_dpm(${jobIngestionId}, ${clientId}, '${type}')`
		)

		return true
	} catch (e) {
		console.log(e.message)
		await advito('job_ingestion_hotel')
			.update('ingestion_note', 'error')
			.where('job_ingestion_id', event.jobIngestionId)
		throw Error('Loading enhanced QC failed')
	}
}

// module.exports.approveFile({
// jobIngestionId: 18862,
// clientId: 348,
// type: 'sourcing'
// })
