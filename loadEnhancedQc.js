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
		const { jobIngestionIds, clientId, year, month, type } = event
		if (!jobIngestionIds.length) {
			throw Error('Job ingestion not found')
		}
		console.log('running for jobs: ', jobIngestionIds)
		const startTime = new Date().getTime()
		console.log(
			'Calling stored procedure: ',
			`SELECT load_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, ${year}, ${month}, '${type}')`
		)
		const { rows } = await advito.raw(
			`SELECT load_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, ${year}, ${month}, '${type}')`
		)
		const startTime2 = new Date().getTime()
		console.log(
			`load_for_sourcing_dpm Run Time: ${(startTime2 - startTime) / 1000}s`
		)
		if (rows.length) {
			const hotelProjectId = rows[0].load_for_sourcing_dpm
			console.log('Hotel project id: ', hotelProjectId)
			await new Promise((done) => setTimeout(() => done(), 3000))
			console.log(
				'Calling stored procedure: ',
				`SELECT load_for_sourcing_dpm_calculate(${hotelProjectId})`
			)
			const startTime3 = new Date().getTime()
			await advito.raw(
				`SELECT load_for_sourcing_dpm_calculate(${hotelProjectId})`
			)
			console.log(
				`load_for_sourcing_dpm_calculate Run Time: ${
					(startTime3 - startTime2) / 1000
				}s`
			)
			await new Promise((done) => setTimeout(() => done(), 3000))
			console.log(
				'Calling stored procedure: ',
				`SELECT best_of_hotel_project_property(${hotelProjectId})`
			)
			const startTime4 = new Date().getTime()
			await advito.raw(
				`SELECT best_of_hotel_project_property(${hotelProjectId})`
			)
			console.log(
				`best_of_hotel_project_property Run Time: ${
					(startTime4 - startTime3) / 1000
				}s`
			)

			console.log('Updating job ingestion hotel status')

			if (type === 'dpm') {
				await advito('job_ingestion_hotel')
					.update({
						is_dpm: true,
						status_dpm: 'Loaded',
						date_status_dpm: new Date()
					})
					.whereIn('job_ingestion_id', jobIngestionIds)
			} else {
				await advito('job_ingestion_hotel')
					.update({
						is_sourcing: true,
						status_sourcing: 'Loaded',
						date_status_sourcing: new Date()
					})
					.whereIn('job_ingestion_id', jobIngestionIds)
			}

			console.log('Updating job ingestion status')

			await advito('job_ingestion')
				.update({
					job_status: 'loaded'
				})
				.whereIn('id', jobIngestionIds)

			console.log('done!')
		} else {
			console.log('Hotel project not found... something went wrong!')
			await advito.destroy()
			throw new Error('Hotel project not found... something went wrong!')
		}
		await advito.destroy()
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
// 	jobIngestionIds: [19009],
// 	clientId: 257,
// 	year: 2021,
// 	month: 'NULL',
// 	type: 'sourcing'
// })
