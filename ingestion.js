'use strict'

const yenv = require('yenv')
const env = yenv('./env.yml', {
	env: 'production'
})
const aws = require('aws-sdk')
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

module.exports.deleteErrorFiles = async () => {
	const date = new Date()
	date.setDate(date.getDate() - 5)
	try {
		console.log('Starting for this date: ', new Date())
		const jobIngestionList = await advito('job_ingestion')
			.select('id', 'file_name')
			.andWhere('job_status', 'error')
			.andWhere('upload_timestamp', '<=', date)

		console.log('jobIngestionList: ', jobIngestionList)

		const deleteArray = jobIngestionList
			.filter((v) => v.file_name)
			.map((v) => ({ Key: v.file_name }))

		if (deleteArray.length) {
			const params = {
				Bucket: process.env.BUCKET ? process.env.BUCKET : env.BUCKET,
				Delete: {
					Objects: deleteArray,
					Quiet: false
				}
			}

			console.log(`Deleting ${deleteArray.length} files.`)

			s3.deleteObjects(params, (err, data) => {
				if (err) console.log(err, err.stack)
				else console.log('deleted files: ', data)
			})

			console.log('Updating list: ', jobIngestionList.length)
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

// module.exports.deleteErrorFiles()
