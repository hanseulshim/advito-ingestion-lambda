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

module.exports.ingestHotelTemplate = async (event) => {
	try {
		const { jobIngestionId, data, start, end, final } = event
		if (!jobIngestionId) throw Error('Job ingestion not found')
		const jobIngestion = await advito('job_ingestion')
			.where('id', jobIngestionId)
			.first()
		if (!jobIngestionId) throw Error('Job ingestion not found')
		const {
			data_start_date: dataStartDate,
			data_end_date: dataEndDate
		} = jobIngestion
		const date1 = new Date(dataStartDate)
		const date2 = new Date(dataEndDate)
		const midpoint = new Date((date1.getTime() + date2.getTime()) / 2)
		console.log('midpoint date: ', midpoint.toLocaleDateString())
		const columnList = await advito('advito_application_template_column as tc')
			.select('tc.column_name', 'tc.stage_column_name', 'i.file_name')
			.leftJoin(
				'advito_application_template_source as ts',
				'ts.advito_application_template_id',
				'tc.advito_application_template_id'
			)
			.leftJoin(
				'job_ingestion as i',
				'i.advito_application_template_source_id',
				'ts.id'
			)
			.where('i.id', jobIngestionId)
		const { rows: currencyList } = await advito.raw(`
		SELECT	currency_code_from, conversion_rate, conversion_date
			FROM		currency_conversion cc
			WHERE		id IN (SELECT id FROM currency_conversion WHERE currency_code_from = cc.currency_code_from AND currency_code_to = 'USD' AND conversion_date <= '${midpoint.toLocaleDateString()}' ORDER BY conversion_date DESC LIMIT 1)
			ORDER BY currency_code_from
		`)
		const parsedData = JSON.parse(data)
		const insertArray = parsedData.map((json) => {
			const returnObj = {
				job_ingestion_id: jobIngestionId
			}
			Object.keys(json).forEach((key) => {
				const column = columnList.find(
					(c) => c.column_name.toLowerCase() === key.trim().toLowerCase()
				)
				if (column !== undefined) {
					if (column.stage_column_name === 'room_spend') {
						let currencyCode = json['source currency code']
							? json['source currency code'].toLowerCase()
							: json['report currency']
							? json['report currency'].toLowerCase()
							: ''
						const currency = currencyList.find(
							(c) => c.currency_code_from.toLowerCase() === currencyCode
						)
						returnObj['room_spend_usd'] = currency
							? String(+json['room spend'] * +currency.conversion_rate)
							: String(+json['room spend'])
					}
					returnObj[column.stage_column_name] = String(json[key]).trim()
				}
			})
			return returnObj
		})
		console.log(`Inserting rows [${start}:${end}] into db`)
		console.log('insert array: ', insertArray)
		console.log('data: ', data)
		const result = await advito('stage_activity_hotel').insert(insertArray)
		if (final) {
			console.log(`Expected to insert ${end - start} rows.`)
			console.log(`Data length is: ${parsedData.length}.`)
			console.log(`Insert length is: ${insertArray.length}.`)
			console.log(`Inserted ${result.rowCount} rows into DB.`)
			console.log('Inserted into job: ', jobIngestionId)
			await advito('job_ingestion')
				.update({
					is_complete: true,
					job_status: 'ingested',
					job_note: `Ingestion Date: ${new Date().toLocaleString()}`
				})
				.where('id', jobIngestionId)
		}
		return `Insert ${jobIngestionId} into successful`
	} catch (e) {
		console.log(e)
		await advito('job_ingestion')
			.update({
				is_complete: true,
				job_status: 'error',
				job_note: `Error with inserting rows: ${e.message}`
			})
			.where('id', event.jobIngestionId)
	}
}

// module.exports.ingestHotelTemplate({
// 	jobIngestionId: 511,
// 	start: 7050,
// 	end: 7161,
// 	final: true,
// 	data: `[
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "OSHEN YEPPOON PL YEPPOON",
// 				"hotel name alternate": null,
// 				"address line 1": "49 HILL STREET",
// 				"address line 2": null,
// 				"city name": "YEPPOON",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "AUSTRALIA",
// 				"country code": "AU",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-08-05T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 3,
// 				"room spend": 500,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "OSHEN YEPPOON PL YEPPOON",
// 				"hotel name alternate": null,
// 				"address line 1": "49 HILL STREET",
// 				"address line 2": null,
// 				"city name": "YEPPOON",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "AUSTRALIA",
// 				"country code": "AU",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-08-05T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 3,
// 				"room spend": 500,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "OUTBACK PIONEER YULARA",
// 				"hotel name alternate": null,
// 				"address line 1": "2 YULARA DRIVE",
// 				"address line 2": null,
// 				"city name": "YULARA",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "AUSTRALIA",
// 				"country code": "AU",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-05-30T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 3,
// 				"room spend": 420,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "HOTEL INTOURIST ZAPOROZHYE",
// 				"hotel name alternate": null,
// 				"address line 1": "SOBORNYI AVE, 135",
// 				"address line 2": null,
// 				"city name": "ZAPORIZHIA OBLAST",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "UKRAINE",
// 				"country code": "UA",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-05-20T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 5,
// 				"room spend": 775.97,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "HOTEL CECIL  OLD MI ZEEHAN",
// 				"hotel name alternate": null,
// 				"address line 1": "99 MAIN STREET",
// 				"address line 2": null,
// 				"city name": "ZEEHAN ",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "AUSTRALIA",
// 				"country code": "AU",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-04-02T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 1,
// 				"room spend": 120,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "CITIZENM B.V. ZUERICH",
// 				"hotel name alternate": null,
// 				"address line 1": "TALACKER 42",
// 				"address line 2": null,
// 				"city name": "ZRICH",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "SWITZERLAND",
// 				"country code": "CH",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-08-12T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 2,
// 				"room spend": 264.74,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		,
// 		{
// 				"chain name": null,
// 				"brand code": null,
// 				"brand name": null,
// 				"internal hotel code": null,
// 				"lanyon id": null,
// 				"sabre property id": null,
// 				"apollo property id": null,
// 				"worldspan property id": null,
// 				"amadeus property id": null,
// 				"hrs id": null,
// 				"merchant id": null,
// 				"hotel name": "CITIZENM B.V. ZUERICH",
// 				"hotel name alternate": null,
// 				"address line 1": "TALACKER 42",
// 				"address line 2": null,
// 				"city name": "ZRICH",
// 				"city latitude": null,
// 				"city longitude": null,
// 				"state code": null,
// 				"country name": "SWITZERLAND",
// 				"country code": "CH",
// 				"zip or postal code": null,
// 				"phone number": null,
// 				"hotel latitude": null,
// 				"hotel longitude": null,
// 				"start date": "2019-01-01T00:00:00.000Z",
// 				"end date": "2019-09-30T00:00:00.000Z",
// 				"transaction date": "2019-08-12T00:00:00.000Z",
// 				"number of rooms": null,
// 				"room rate": null,
// 				"transaction currency": "AUD",
// 				"report currency": "AUD",
// 				"currency conversion rate": null,
// 				"number of transactions": 2,
// 				"room spend": 264.74,
// 				"fees and taxes": null,
// 				"f&b spend": null,
// 				"other spend": null,
// 				"total hotel spend": null
// 		}
// 		]`
// })
