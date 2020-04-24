'use strict'

const aws = require('aws-sdk')
const csv = require('csv-parser')
const s3 = new aws.S3({
	apiVersion: '2006-03-01',
	accessKeyId: process.env.ACCESS_KEY_ID,
	secretAccessKey: process.env.SECRET_ACCESS_KEY,
	region: process.env.REGION
})
const advito = require('knex')({
	client: 'pg',
	connection: {
		user: process.env.DB_USER,
		host: process.env.DB_HOST,
		database: process.env.DB_ADVITO,
		password: process.env.DB_PASSWORD
	}
})
const hotel = require('knex')({
	client: 'pg',
	connection: {
		user: process.env.DB_USER,
		host: process.env.DB_HOST,
		database: process.env.DB_HOTEL,
		password: process.env.DB_PASSWORD
	}
})

module.exports.insertBcdStageMonthlyRates = async (event) => {
	const bucket = event.Records[0].s3.bucket.name
	const key = decodeURIComponent(
		event.Records[0].s3.object.key.replace(/\+/g, ' ')
	)
	const fileName = key.split('/')[1]
	const fileSize = event.Records[0].s3.object.size

	const params = {
		Bucket: bucket,
		Key: key
	}

	try {
		const results = []
		const s3Stream = s3.getObject(params).createReadStream()
		return new Promise((resolve, reject) => {
			s3Stream
				.pipe(csv())
				.on('data', (data) => results.push(data))
				.on('end', async () => {
					const lookupTable = await advito('advito_application_template_column')
						.select('column_name', 'stage_column_name')
						.where('advito_application_template_id', 19)
					const queryArray = []
					const errorArray = []

					for (const row of results) {
						const queryObject = {}
						for (let [col, value] of Object.entries(row)) {
							const lookup = lookupTable.find(
								(v) => v.column_name === col.trim()
							)
							if (lookup) {
								queryObject[lookup.stage_column_name] = value
								continue
							} else if (errorArray.indexOf(col.trim()) === -1) {
								errorArray.push(col.trim())
							}
						}
						queryArray.push(queryObject)
					}
					await advito('job_ingestion')
						.insert({
							advito_user_id: 882,
							client_id: 1,
							advito_application_template_source_id: 58,
							data_start_date: new Date(),
							data_end_date: new Date(),
							original_file_name: fileName,
							count_rows: queryArray.length,
							file_extension: 'csv',
							file_size: fileSize,
							is_complete: true,
							job_status: 'done',
							processing_start_timestamp: new Date(),
							job_note: errorArray.length
								? `Error from in these columns: ${errorArray}`
								: ''
						})
						.returning('id')
					await hotel('stage_bcd_rate').insert(queryArray)
					resolve(true)
				})
				.on('error', (e) => {
					console.error('Stream error:', e)
					reject(e)
				})
		})
	} catch (e) {
		console.log(`Error: ${e.message}`)
	}
}
