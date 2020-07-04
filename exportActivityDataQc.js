'use strict'
const { parse } = require('json2csv')
const yenv = require('yenv')
const env = yenv('./env.yml', {
	env: 'production'
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

module.exports.exportActivityDataQc = async (event, context) => {
	context.callbackWaitsForEmptyEventLoop = false
	try {
		const { jobIngestionIds, currencyType } = event
		if (!jobIngestionIds.length) {
			throw Error('Job ingestion not found')
		}
		console.log('Starting export_stage_activity_hotel_qc')
		const startTime = new Date().getTime()
		const { rows } = await advito.raw(
			`select * from export_stage_activity_hotel_qc(ARRAY[${jobIngestionIds}], '${currencyType}')`
		)
		console.log(
			`Finish export_stage_activity_hotel_qc - run time: ${
				(new Date().getTime() - startTime) / 1000
			}s`
		)

		console.log('deleting export_qc')
		await advito('export_qc')
			.delete()
			.where('job_ingestion_ids', jobIngestionIds.sort().join(', '))
		console.log('inserting export_qc')
		await advito('export_qc').insert({
			export_type: 'activity',
			export_data: rows.length ? parse(rows) : '',
			job_ingestion_ids: jobIngestionIds.sort().join(', ')
		})
		console.log('done')

		return true
	} catch (e) {
		throw e
	}
}

// module.exports.exportActivityDataQc(
// 	{
// 		jobIngestionIds: [727, 597],
// 		currencyType: 'USD'
// 	},
// 	{}
// )
