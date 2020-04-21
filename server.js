'use strict'

const aws = require('aws-sdk')
const yenv = require('yenv')

const env = yenv('./env.yml', {
	env: 'default'
})

const s3 = new aws.S3({
	apiVersion: '2006-03-01',
	accessKeyId: env.ACCESS_KEY_ID,
	secretAccessKey: env.SECRET_ACCESS_KEY,
	region: env.REGION
})

const advito = require('knex')({
	client: 'pg',
	connection: {
		user: env.DB_USER,
		host: env.DB_HOST,
		database: env.DB_ADVITO,
		password: env.DB_PASSWORD
	}
})

const processFile = async () => {
	const date = new Date()
	date.setDate(date.getDate() - 5)
	try {
		const jobIngestionList = await advito('job_ingestion')
			.select('id', 'file_name')
			.andWhere('job_status', 'error')
			.andWhere('upload_timestamp', '<=', date)

		const deleteArray = jobIngestionList
			.filter((v) => v.file_name)
			.map((v) => ({ Key: v.file_name }))

		console.log(jobIngestionList, deleteArray)
		if (deleteArray.length) {
			const params = {
				Bucket: env.BUCKET,
				Delete: {
					Objects: deleteArray,
					Quiet: false
				}
			}

			s3.deleteObjects(params, (err, data) => {
				if (err) console.log(err, err.stack)
				else console.log('deleted files: ', data)
			})

			const updateList = jobIngestionList.map((job) =>
				advito('job_ingestion')
					.where('id', job.id)
					.update({
						job_status: 'deleted',
						job_note: `Template Removed: ${new Date().toLocaleString()}`
					})
			)

			console.log(updateList)
			await Promise.all(updateList)
		}
	} catch (e) {
		console.log(`Error: ${e.message}`)
	}
}

processFile()
