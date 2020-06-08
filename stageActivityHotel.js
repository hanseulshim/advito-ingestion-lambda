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
		SELECT	currency_code_to, conversion_rate_reverse
			FROM		currency_conversion cc
			WHERE		id IN (SELECT id FROM currency_conversion WHERE currency_code_to = cc.currency_code_to ORDER BY conversion_date DESC LIMIT 1)
			ORDER BY currency_code_to
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
						const currency = currencyList.find((c) =>
							c.currency_code_to.toLowerCase() === json['source currency code']
								? json['source currency code'].toLowerCase()
								: ''
						)
						returnObj['room_spend_usd'] = currency
							? +json['room spend'] * +currency.conversion_rate_reverse
							: +json['room spend']
					}
					returnObj[column.stage_column_name] = String(json[key]).trim()
				}
			})
			return returnObj
		})
		console.log(`Inserting rows [${start}:${end}] into db`)
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
			.where('id', jobIngestionId)
	}
}

module.exports.ingestHotelTemplate({
	jobIngestionId: 18758,
	start: 0,
	end: 15,
	final: true,
	data:
		'[{"Client Name":"CloudVenture", "Room Spend": 123.456, "Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Australia","Agency Code":"X8","Booking Source":"Agency","Local Hotel Reason Code Description":"Customer Preferred Hotel Booked","Global Hotel Reason Code Description":"Preferred Hotel Booked","Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Australia","Ticketing Country":"Australia","Ticketing Region":"South Pacific","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"CY","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Courtyard","Property Name":"Courtyard by Marriott West Palm Beach Airport","Property Address 1":"1800 Centrepark Drive East","Property Address 2":null,"Property City":"West Palm Beach","Property City Code":"PBI","Major City":"West Palm Beach","Property State":"FL","Postal Code":33401,"Property Country Code":"US","Property Country":"United States","Phone":15612071800,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F1K","Room Type":"1 KING BED               ","Rate Category":"BAR","Room Rate":330.2565,"Total Amount ":660.513,"Hotel Preferred Indicator":"Y","Booking Date":"2019-04-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":907.497,"Lowest Rate Available":314.53,"Source Currency Code":"USD","Transaction Currency":"JPY","Currency Conversion Rate":0.0,"Amadeus ID":"PBICYC","Worldspan ID":"PBICY","Sabre ID":58685,"Apollo ID":36413,"HRS ID":null,"Lanyon ID":78758,"Internal Hotel Code":"PBICY","City Latitude":26.714439,"City Longitude":-80.054947,"Hotel Latitude":26.6948451996,"Hotel Longitude":-80.0698776245,"Client Field 1":"EAST","Client Field 2":null,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null}]'
})
