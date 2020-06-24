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

module.exports.backout = async (event) => {
	try {
		const { jobIngestionId } = event
		const statuses = ['processed', 'loaded', 'approved']
		const startTime = new Date().getTime()
		console.log('running for job: ', jobIngestionId)

		const jobIngestion = await advito('job_ingestion')
			.select()
			.where('id', jobIngestionId)
			.first()

		if (!jobIngestion) {
			throw new TypeError('Job Ingestion Hotel not found', '500')
		} else if (
			!jobIngestion.is_complete ||
			!statuses.includes(jobIngestion.job_status)
		) {
			throw new Error('Job Ingestion does not have status of complete', '500')
		} else if (!statuses.includes(jobIngestion.job_status)) {
			throw new Error('Job Ingestion does not have status of complete', '500')
		} else if (!statuses.includes(jobIngestion.job_status)) {
			throw new Error(
				'Job Ingestion does not have one of the following status: processed, loaded, approved.',
				'500'
			)
		}

		const stageActivityHotelList = await advito('stage_activity_hotel')
			.where('job_ingestion_id', jobIngestion.id)
			.select('id')

		const hotelProjectPropertyList = await advito('hotel_project_property')
			.select()
			.where('agency_job_ingestion_id', jobIngestion.id)
			.orWhere('cc_job_ingestion_id', jobIngestion.id)
			.orWhere('supplier_job_ingestion_id', jobIngestion.id)

		const stageActivityHotelIds = stageActivityHotelList.map((v) => v.id)

		const startTime1 = new Date().getTime()
		console.log(
			`Grabbed both lists. stageActivityHotelList length: ${
				stageActivityHotelList.length
			}, hotelProjectPropertyList length: ${
				hotelProjectPropertyList.length
			}. Run Time: ${startTime1 - startTime}ms`
		)

		await Promise.all([
			advito('activity_hotel')
				.delete()
				.whereIn('stage_id', stageActivityHotelIds),
			advito('stage_activity_hotel_candidate')
				.delete()
				.whereIn('stage_activity_hotel_id', stageActivityHotelIds)
		])
		const startTime2 = new Date().getTime()
		console.log(
			`Deleted activity_hotel and stage_activity_hotel_candidate. Run Time: ${
				startTime2 - startTime
			}ms`
		)

		const agencyList = hotelProjectPropertyList.filter(
			(v) => v.agencyJobIngestionId !== null
		)
		const ccList = hotelProjectPropertyList.filter(
			(v) => v.ccJobIngestionId !== null
		)
		const supplierList = hotelProjectPropertyList.filter(
			(v) => v.supplierJobIngestionId !== null
		)

		await Promise.all([
			advito('stage_activity_hotel')
				.delete()
				.whereIn('id', stageActivityHotelIds),
			advito('hotel_project_property_day')
				.delete()
				.whereIn('hotel_property_id', [
					...new Set(agencyList.map((v) => v.hotel_property_id))
				])
				.whereIn('hotel_project_id', [
					...new Set(agencyList.map((v) => v.hotel_project_id))
				])
				.whereRaw('LOWER("template_category") = ?', 'agency'),
			advito('hotel_project_property_day')
				.delete()
				.whereIn('hotel_property_id', [
					...new Set(ccList.map((v) => v.hotel_property_id))
				])
				.whereIn('hotel_project_id', [
					...new Set(ccList.map((v) => v.hotel_project_id))
				])
				.whereRaw('LOWER("template_category") = ?', 'cc'),
			advito('hotel_project_property_day')
				.delete()
				.whereIn('hotel_property_id', [
					...new Set(supplierList.map((v) => v.hotel_property_id))
				])
				.whereIn('hotel_project_id', [
					...new Set(supplierList.map((v) => v.hotel_project_id))
				])
				.whereRaw('LOWER("template_category") = ?', 'supplier'),
			advito('hotel_project_property')
				.update({
					agency_date_currency_conversion: null,
					agency_room_nights: null,
					agency_spend_usd: null
				})
				.where('agency_job_ingestion_id', jobIngestion.id),
			advito('hotel_project_property')
				.update({
					cc_date_currency_conversion: null,
					cc_room_nights: null,
					cc_spend_usd: null
				})
				.where('cc_job_ingestion_id', jobIngestion.id),
			advito('hotel_project_property')
				.update({
					supplier_date_currency_conversion: null,
					supplier_room_nights: null,
					supplier_spend_usd: null
				})
				.where('supplier_job_ingestion_id', jobIngestion.id)
		])
		console.log(
			`Deleted everything else. Run Time: ${
				new Date().getTime() - startTime2
			}ms`
		)
		return true
	} catch (e) {
		if (e.name !== 'TypeError') {
			const { jobIngestionId, jobStatus } = event
			await advito('job_ingestion')
				.update({
					job_status: jobStatus
				})
				.where('id', jobIngestionId)
		}
		throw e
	}
}

// module.exports.backout({
// 	jobIngestionId: 18654,
// 	jobStatus: 'test'
// })
