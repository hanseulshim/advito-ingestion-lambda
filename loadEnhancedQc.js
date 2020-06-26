'use strict'
const aws = require('aws-sdk')
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
const lambda = new aws.Lambda({
	accessKeyId: process.env.ACCESS_KEY_ID
		? process.env.ACCESS_KEY_ID
		: env.ACCESS_KEY_ID,
	secretAccessKey: process.env.SECRET_ACCESS_KEY
		? process.env.SECRET_ACCESS_KEY
		: env.SECRET_ACCESS_KEY,
	region: process.env.REGION ? process.env.REGION : env.REGION
})

module.exports.loadEnhancedQc = async (event) => {
	try {
		const { jobIngestionIds, clientId, year, month, type } = event
		if (!jobIngestionIds.length) {
			throw Error('Job ingestion not found')
		}
		console.log('running for jobs: ', jobIngestionIds)
		const startTime = new Date().getTime()
		console.log(
			'Calling stored procedure: ',
			`select load_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, ${year}, ${month}, '${type}')`
		)
		const { rows } = await advito.raw(
			`select load_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, ${year}, ${month}, '${type}')`
		)
		console.log(`Load Run Time: ${new Date().getTime() - startTime}ms`)

		console.log('Load result: ', rows)

		await advito.destroy()

		const params = {
			FunctionName:
				process.env.ENVIRONMENT === 'PRODUCTION'
					? 'advito-ingestion-production-best-of-logic'
					: process.env.ENVIRONMENT === 'STAGING'
					? 'advito-ingestion-staging-best-of-logic'
					: 'advito-ingestion-dev-best-of-logic',
			InvocationType: 'Event',
			Payload: JSON.stringify({
				hotelProjectId: rows[0].load_for_sourcing_dpm
			})
		}
		await lambda.invoke(params).promise()

		return true
	} catch (e) {
		console.log(e.message)
		await advito('job_ingestion_hotel')
			.update('ingestion_note', 'error')
			.whereIn('job_ingestion_id', event.jobIngestionIds)
		throw e
	}
}

// module.exports.loadEnhancedQc({
// 	jobIngestionIds: [18890],
// 	clientId: 348,
// 	year: 2019,
// 	month: 'NULL',
// 	type: 'sourcing'
// })
