'use strict'

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

const hotel = require('knex')({
	client: 'pg',
	connection: {
		user: process.env.DB_USER ? process.env.DB_USER : env.DB_USER,
		host: process.env.DB_HOST ? process.env.DB_HOST : env.DB_HOST,
		database: process.env.DB_HOTEL ? process.env.DB_HOTEL : env.DB_HOTEL,
		password: process.env.DB_PASSWORD
			? process.env.DB_PASSWORD
			: env.DB_PASSWORD
	}
})

module.exports.approveFile = async (event, context) => {
	context.callbackWaitsForEmptyEventLoop = false
	try {
		const { jobIngestionIds, clientId, type, userId } = event
		if (!jobIngestionIds.length) {
			throw Error('Job ingestion not found')
		}
		console.log(
			`SELECT * FROM approve_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, '${type}')`
		)
		const { rows } = await advito.raw(
			`SELECT * FROM approve_for_sourcing_dpm(ARRAY[${jobIngestionIds}], ${clientId}, '${type}')`
		)
		console.log('return from approve_for_sourcing_dpm: ', rows)
		if (rows.length) {
			const hotelProject = rows[0]
			if (type === 'dpm') {
				console.log(`SELECT process_hpd(${hotelProject.id}, ${userId})`)
				await hotel.raw(`SELECT process_hpd(${hotelProject.id}, ${userId})`)
			} else {
				console.log(
					`SELECT process_spend_analysis(${hotelProject.id}, ${userId})`
				)
				await hotel.raw(
					`SELECT process_spend_analysis(${hotelProject.id}, ${userId})`
				)
				console.log(`SELECT approve_hpm(${hotelProject.id})`)
				await hotel.raw(`SELECT approve_hpm(${hotelProject.id})`)
				console.log(`SELECT update_bar_rates(${hotelProject.project_year})`)
				await hotel.raw(`SELECT update_bar_rates(${hotelProject.project_year})`)
				console.log(`SELECT update_bcd_rates(${hotelProject.project_year})`)
				await hotel.raw(`SELECT update_bcd_rates(${hotelProject.project_year})`)
			}
		} else {
			console.log('Hotel project not found... something went wrong!')
			throw new Error('Hotel project not found... something went wrong!')
		}

		console.log('done')

		return true
	} catch (e) {
		console.log(e.message)
		await advito('job_ingestion_hotel')
			.update('ingestion_note', 'error')
			.whereIn('job_ingestion_id', event.jobIngestionIds)
		throw Error('Loading enhanced QC failed')
	}
}

// module.exports.approveFile(
// 	{
// 		jobIngestionIds: [801],
// 		clientId: 348,
// 		type: 'sourcing',
// 		userId: 882
// 	},
// 	{}
// )
