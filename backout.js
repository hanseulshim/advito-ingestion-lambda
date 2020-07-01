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

module.exports.backout = async (event, context) => {
	context.callbackWaitsForEmptyEventLoop = false
	try {
		const { jobIngestionId } = event
		console.log('running for job: ', jobIngestionId)

		const hotelProject = await advito('hotel_project_job_ingestion')
			.select()
			.where('job_ingestion_id', jobIngestionId)
			.first()

		console.log('hotelProject', hotelProject)

		if (hotelProject) {
			console.log('Starting hotel project deletions')
			const startTime = new Date().getTime()
			const jobIngestionList = await advito('hotel_project_job_ingestion')
				.select()
				.where('hotel_project_id', hotelProject.hotel_project_id)

			console.log('JobIngestionList: ', jobIngestionList)

			if (jobIngestionList.length) {
				await advito('job_ingestion_hotel')
					.update({
						is_dpm: false,
						status_dpm: null,
						date_status_dpm: null,
						is_sourcing: false,
						status_sourcing: null,
						date_status_sourcing: null,
						ingestion_note: null
					})
					.whereIn(
						'job_ingestion_id',
						jobIngestionList.map((v) => v.job_ingestion_id)
					)
				console.log('Finish job ingestion hotel status updates')
			}
			await advito('hotel_project_property_day')
				.delete()
				.where('hotel_project_id', hotelProject.hotel_project_id)
			console.log('hotel_project_property_day delete done')
			await advito('hotel_project_property')
				.delete()
				.where('hotel_project_id', hotelProject.hotel_project_id)
			console.log('hotel_project_property delete done')
			await advito('hotel_project_job_ingestion')
				.delete()
				.where('hotel_project_id', hotelProject.hotel_project_id)
			console.log('hotel_project_job_ingestion delete done')
			await advito('hotel_project')
				.delete()
				.where('id', hotelProject.hotel_project_id)
			console.log('hotel_project delete done')
			console.log(
				`Deleted hotel project stuff. Run Time: ${
					new Date().getTime() - startTime
				}ms`
			)
		} else {
			console.log(
				'No hotel project. Just updating single job_ingestion_hotel status'
			)
			await advito('job_ingestion_hotel')
				.update({
					is_dpm: false,
					status_dpm: null,
					date_status_dpm: null,
					is_sourcing: false,
					status_sourcing: null,
					date_status_sourcing: null,
					ingestion_note: null
				})
				.where('job_ingestion_id', jobIngestionId)
			console.log('Finish job ingestion hotel status updates')
		}

		console.log('Starting other deletes')
		const startTime2 = new Date().getTime()
		const stageActivityHotelList = await advito('stage_activity_hotel')
			.where('job_ingestion_id', jobIngestionId)
			.select('id')
		const stageActivityHotelIds = stageActivityHotelList.map((v) => v.id)

		await Promise.all([
			advito('activity_hotel')
				.delete()
				.whereIn('stage_id', stageActivityHotelIds),
			advito('stage_activity_hotel_candidate')
				.delete()
				.whereIn('stage_activity_hotel_id', stageActivityHotelIds)
		])
		console.log('Deleted activity_hotel and stage_activity_hotel_candidate')
		await advito('stage_activity_hotel')
			.where('job_ingestion_id', jobIngestionId)
			.delete()
		console.log('Deleted stage_activity_hotel')

		await advito('job_ingestion')
			.update({ job_status: 'backout' })
			.where('id', jobIngestionId)

		console.log(
			`Backout done. Run Time: ${new Date().getTime() - startTime2}ms`
		)

		return true
	} catch (e) {
		throw e
	}
}

// module.exports.backout({
// 	jobIngestionId: 18986
// })
