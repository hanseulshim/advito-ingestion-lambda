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

module.exports.ingestHotelTemplate = async (event, context) => {
	context.callbackWaitsForEmptyEventLoop = false
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
							? String(+json[key] * +currency.conversion_rate)
							: String(+json[key])
					}
					if (column.stage_column_name === 'traveler_last_name') {
						const travelerLastName = json['traveler last name']
						const travelerFirstName = json['traveler first name']
						returnObj['traveler'] = `${travelerLastName}/${travelerFirstName}`
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
// 	jobIngestionId: 1041,
// 	start: 7050,
// 	end: 7161,
// 	final: true,
// 	data: ` [{"client name":"PITNEY BOWES CANADA ","global customer number":null,"locator":"LCLNZU ","traveler":"MCELVEEN\/CAMERON R W","invoice date":"2019-07-10T00:00:00.000Z","invoice number":3694,"agency name":"BCD Canada","agency code":"IC","booking source":"Online","local hotel reason code":"HE","global hotel reason code":"No Pref Hotel","refund indicator":"N","exchange indicator":"N","original document number":null,"int dom":"TransBorder","traveler country":"Canada","ticketing country":"Canada","ticketing region":null,"check-in date":"2019-08-05T00:00:00.000Z","check-out date":"2019-08-07T00:00:00.000Z","confirmation number":"97094702 ","hotel chain code":"Hilton Worldwide","hotel chain name":"HG","hotel brand name":"Homewood Suites","property name":"Homewood Suites By Hilton Nashville - Downtown","property address 1":"706 Church Street","property address 2":null,"property city":"Nashville","property city code":"BNA","major city":"Nashville","property state":"TN","property postal code":"37203","property country code":"US","property country":"United States","property phone":16157425550,"number of rooms":1,"number of nights":2,"room type code":"A04","room type":"SMOKING KING ","rate category":"C0Z","room rate":296.64,"total amount":593.28,"hotel preferred indicator":"N","booking date":null,"service fees":null,"highest rate available":null,"lowest rate available":null,"source currency code":"USD","transaction currency code":"USD","conversion rate":null,"amadeus id":"HGBNA706","worldspan id":"HG5680","sabre id":48693.0,"apollo id":"66191","hrs id":null,"lanyon id":null,"internal hotel code":null,"city latitude":null,"city longitude":null,"hotel latitude":null,"hotel longitude":null,"client field 1":null,"client field 2":null,"client field 3":null,"client field 4":null,"client field 5":null,"client field 6":null,"client field 7":null,"client field 8":null,"client field 9":null,"client field 10":null}]`
// })
