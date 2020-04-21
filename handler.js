'use strict'

const aws = require('aws-sdk')
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

module.exports.cleanupFile = async () => {
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

		if (deleteArray.length) {
			const params = {
				Bucket: process.env.BUCKET,
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
			await Promise.all(updateList)
		}
	} catch (e) {
		console.log(`Error: ${e.message}`)
	}
}
