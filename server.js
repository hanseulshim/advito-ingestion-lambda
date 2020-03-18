'use strict'

const aws = require('aws-sdk')
const csv = require('csv-parser')
const fs = require('fs')
const s3 = new aws.S3({
	apiVersion: '2006-03-01',
	accessKeyId: process.env.ACCESS_KEY_ID,
	secretAccessKey: process.env.SECRET_ACCESS_KEY,
	region: process.env.REGION
})
const DB_HOST = 'air-dev.cw0g3uulplsy.us-east-2.rds.amazonaws.com'
const DB_USER = 'AdvitoAdmin'
const DB_PASSWORD = 'B!3k_8cr'
const DB_ADVITO = 'advito'
const DB_HOTEL = 'hotel'
const advito = require('knex')({
	client: 'pg',
	connection: {
		user: DB_USER,
		host: DB_HOST,
		database: DB_ADVITO,
		password: DB_PASSWORD
	}
})
const hotel = require('knex')({
	client: 'pg',
	connection: {
		user: DB_USER,
		host: DB_HOST,
		database: DB_HOTEL,
		password: DB_PASSWORD
	}
})

const processFile = async event => {
	// const bucket = event.Records[0].s3.bucket.name
	// const key = decodeURIComponent(
	// 	event.Records[0].s3.object.key.replace(/\+/g, ' ')
	// )

	// const params = {
	// 	Bucket: bucket,
	// 	Key: key
	// }

	try {
		// const s3Data = await s3.getObject(params).createReadStream()

		const lookupTable = await advito('advito_application_template_column')
			.select('column_name', 'stage_column_name')
			.where('advito_application_template_id', 19)

		const queryArray = []
		const errorArray = []

		const results = []

		fs.createReadStream('./test.csv')
			.pipe(csv())
			.on('data', data => results.push(data))
			.on('end', async () => {
				for (const row of results) {
					const queryObject = {}
					for (let [col, value] of Object.entries(row)) {
						const lookup = lookupTable.find(v => v.column_name === col.trim())
						if (lookup) {
							queryObject[lookup.stage_column_name] = value
							continue
						} else if (errorArray.indexOf(col.trim()) === -1) {
							errorArray.push(col.trim())
						}
					}
					queryArray.push(queryObject)
				}
				const job = await advito('job_ingestion')
					.insert({
						advito_user_id: 882,
						client_id: 1,
						advito_application_template_source_id: 58,
						data_start_date: new Date(),
						data_end_date: new Date(),
						original_file_name: 'test.csv',
						count_rows: queryArray.length,
						file_extension: 'csv',
						file_size: 1000,
						is_complete: true,
						job_status: 'done',
						processing_start_timestamp: new Date(),
						job_note: errorArray.length
							? `Error from in these columns: ${errorArray}`
							: ''
					})
					.returning('id')
				await hotel('stage_bcd_rate').insert(queryArray)
				console.log(job[0])
			})
	} catch (e) {
		console.log(`Error: ${e.message}`)
	}
}

processFile()
