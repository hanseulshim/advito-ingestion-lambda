'use strict'
const { parse } = require('json2csv')
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

module.exports.exportActivityDataQc = async (event, context) => {
	context.callbackWaitsForEmptyEventLoop = false
	try {
		const { clientId, dataStartDate, dataEndDate, currencyType } = event

		console.log('Starting export_stage_activity_hotel_qc')
		const startTime = new Date().getTime()
		const { rows } = await advito.raw(
			`select * from export_stage_activity_hotel_qc(${clientId}, '${dataStartDate}'::date, '${dataEndDate}'::date, '${currencyType}')`
		)
		console.log(
			`Finish export_stage_activity_hotel_qc - run time: ${
				(new Date().getTime() - startTime) / 1000
			}s`
		)
		console.log('deleting export_qc')
		await advito('export_qc')
			.delete()
			.where('client_id', clientId)
			.andWhere('export_type', 'activity')
			.andWhere('data_start_date', dataStartDate)
			.andWhere('data_end_date', dataEndDate)
		console.log('inserting export_qc')
		await advito('export_qc').insert({
			client_id: clientId,
			export_type: 'activity',
			export_data: rows.length ? parse(rows) : '',
			data_start_date: dataStartDate,
			data_end_date: dataEndDate
		})
		console.log('done')

		return true
	} catch (e) {
		throw e
	}
}

// module.exports.exportActivityDataQc({
// 	clientId: 226,
// 	dataStartDate: '01-01-2019',
// 	dataEndDate: '12-31-2020',
// 	currencyType: 'USD'
// })
