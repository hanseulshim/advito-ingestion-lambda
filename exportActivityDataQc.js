'use strict'
const aws = require('aws-sdk')
const { parse } = require('json2csv')
const yenv = require('yenv')
const env = yenv('./env.yml', {
  env: 'production'
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
const Bucket = 'advito-ingestion-templates'

module.exports.exportActivityDataQc = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false
  try {
    const { jobIngestionIds, currencyType } = event
    if (!jobIngestionIds.length) {
      throw Error('Job ingestion not found')
    }
    console.log(
      'Starting export_stage_activity_hotel_qc: ',
      `select * from export_stage_activity_hotel_qc(ARRAY[${jobIngestionIds}], '${currencyType}')`
    )
    const startTime = new Date().getTime()
    const { rows } = await advito.raw(
      `select * from export_stage_activity_hotel_qc(ARRAY[${jobIngestionIds}], '${currencyType}')`
    )
    console.log(
      `Finish export_stage_activity_hotel_qc - run time: ${
        (new Date().getTime() - startTime) / 1000
      }s`
    )

    const Key = `csv/${new Date().getTime()}_activity_${jobIngestionIds.join(
      '_'
    )}.csv`
    const Body = parse(rows)
    const params = {
      Bucket,
      Key,
      Body
    }

    await s3.putObject(params).promise()

    console.log('deleting export_qc')
    await advito('export_qc')
      .delete()
      .where('job_ingestion_ids', jobIngestionIds.sort().join(', '))
    console.log('inserting export_qc')
    await advito('export_qc').insert({
      export_type: 'activity',
      export_data: Key,
      job_ingestion_ids: jobIngestionIds.sort().join(', ')
    })
    console.log('done', Key)

    return true
  } catch (e) {
    throw e
  }
}

// module.exports.exportActivityDataQc(
//   {
//     jobIngestionIds: [972],
//     currencyType: 'USD'
//   },
//   {}
// )
