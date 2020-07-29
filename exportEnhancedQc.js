'use strict'
const aws = require('aws-sdk')
const { parse } = require('json2csv')
const yenv = require('yenv')
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
const Bucket = 'advito-ingestion-templates'

module.exports.exportEnhancedQc = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false
  try {
    const { clientId, dataStartDate, dataEndDate, currencyType } = event

    console.log('Starting export_enhanced_qc')
    const startTime = new Date().getTime()
    const { rows } = await advito.raw(
      `select * from export_enhanced_qc(${clientId}, '${dataStartDate}'::date, '${dataEndDate}'::date, '${currencyType}')`
    )
    console.log(
      `Finish export_enhanced_qc - run time: ${
        (new Date().getTime() - startTime) / 1000
      }s`
    )

    const Key = `csv/${new Date().getTime()}_enhanced_${clientId}.csv`
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
      .where('client_id', clientId)
      .andWhere('export_type', 'enhanced')
      .andWhere('data_start_date', dataStartDate)
      .andWhere('data_end_date', dataEndDate)
    console.log('inserting export_qc')
    await advito('export_qc').insert({
      client_id: clientId,
      export_type: 'enhanced',
      export_data: Key,
      data_start_date: dataStartDate,
      data_end_date: dataEndDate
    })
    console.log('done')

    return true
  } catch (e) {
    throw e
  }
}

// module.exports.exportEnhancedQc({
// 	clientId: 257,
// 	dataStartDate: '01-01-2019',
// 	dataEndDate: '12-31-2020',
// 	currencyType: 'USD'
// })
