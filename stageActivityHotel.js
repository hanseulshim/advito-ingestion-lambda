'use strict'

const aws = require('aws-sdk')
const yenv = require('yenv')
const xlsx = require('xlsx')

const env = yenv('./env.yml', {
	env: 'default'
})
const s3 = new aws.S3({
	apiVersion: '2006-03-01',
	accessKeyId: process.env.ACCESS_KEY_ID
		? process.env.ACCESS_KEY_ID
		: env.ACCESS_KEY_ID,
	secretAccessKey: process.env.SECRET_ACCESS_KEY
		? process.env.SECRET_ACCESS_KEY
		: env.SECRET_ACCESS_KEY,
	region: process.env.REGION ? process.env.REGION : env.REGION
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
		const { jobIngestionId } = event
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
		const params = {
			Bucket: 'advito-ingestion-templates',
			Key: columnList[0].file_name
		}

		const file = s3.getObject(params).createReadStream()
		const buffers = []

		file.on('data', (data) => {
			buffers.push(data)
		})

		file.on('end', async () => {
			const buffer = Buffer.concat(buffers)
			const workbook = xlsx.read(buffer)
			const sheetNameList = workbook.SheetNames
			const data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetNameList[0]])

			const insertArray = data.map((json) => {
				const returnObj = {
					job_ingestion_id: jobIngestionId
				}
				Object.keys(json).forEach((key) => {
					const column = columnList.find(
						(c) => c.column_name.toLowerCase() === key.trim().toLowerCase()
					)
					if (column !== undefined) {
						returnObj[column.stage_column_name] = String(
							json[column.column_name]
						).trim()
					}
				})
				return returnObj
			})
			await advito('stage_activity_hotel').insert(insertArray)
			await advito('job_ingestion').update({
				job_status: 'Ingested',
				job_note: `Ingestion Date: ${new Date().toLocaleString()}`
			})
		})
	} catch (e) {
		console.log(e)
	}
}

// module.exports.ingestHotelTemplate({ jobIngestionId: 18378 })
