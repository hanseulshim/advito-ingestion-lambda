'use strict'

const yenv = require('yenv')

const env = yenv('./env.yml', {
	env: 'production'
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
		const parsedData = JSON.parse(data)
		const insertArray = parsedData.map((json) => {
			const returnObj = {
				job_ingestion_id: jobIngestionId
			}
			Object.keys(json).forEach((key) => {
				const column = columnList.find(
					(c) => c.column_name.toLowerCase() === key
				)
				if (column !== undefined) {
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
	}
}

// module.exports.ingestHotelTemplate({
// 	jobIngestionId: 329,
// 	start: 0,
// 	end: 15,
// 	data:
// 		'[{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Australia","Agency Code":"X8","Booking Source":"Agency","Local Hotel Reason Code Description":"Customer Preferred Hotel Booked","Global Hotel Reason Code Description":"Preferred Hotel Booked","Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Australia","Ticketing Country":"Australia","Ticketing Region":"South Pacific","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"CY","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Courtyard","Property Name":"Courtyard by Marriott West Palm Beach Airport","Property Address 1":"1800 Centrepark Drive East","Property Address 2":null,"Property City":"West Palm Beach","Property City Code":"PBI","Major City":"West Palm Beach","Property State":"FL","Postal Code":33401,"Property Country Code":"US","Property Country":"United States","Phone":15612071800,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F1K","Room Type":"1 KING BED               ","Rate Category":"BAR","Room Rate":330.2565,"Total Amount ":660.513,"Hotel Preferred Indicator":"Y","Booking Date":"2019-04-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":907.497,"Lowest Rate Available":314.53,"Source Currency Code":"USD","Transaction Currency Code":"USD","Currency Conversion Rate":0.0,"Amadeus ID":"PBICYC","Worldspan ID":"PBICY","Sabre ID":58685,"Apollo ID":36413,"HRS ID":null,"Lanyon ID":78758,"Internal Hotel Code":"PBICY","City Latitude":26.714439,"City Longitude":-80.054947,"Hotel Latitude":26.6948451996,"Hotel Longitude":-80.0698776245,"Client Field 1":"EAST","Client Field 2":null,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"EABCDE","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Australia","Agency Code":"X8","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Australia","Ticketing Country":"Australia","Ticketing Region":"South Pacific","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"RG","Hotel Chain Name":"Rydges Hotel Group","Hotel Brand Name":"Rydges","Property Name":"Rydges Wellington","Property Address 1":"75 Featherston Street","Property Address 2":null,"Property City":"Wellington","Property City Code":"WLG","Major City":"Wellington","Property State":null,"Postal Code":"6011","Property Country Code":"NZ","Property Country":"New Zealand","Phone":6444998,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F1K","Room Type":"1 KING BED               ","Rate Category":"BAR","Room Rate":163.7654,"Total Amount ":327.5308,"Hotel Preferred Indicator":null,"Booking Date":"2019-03-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":155.967,"Lowest Rate Available":155.967,"Source Currency Code":"USD","Transaction Currency Code":"USD","Currency Conversion Rate":0.0,"Amadeus ID":"WLG391","Worldspan ID":913,"Sabre ID":26318,"Apollo ID":21600,"HRS ID":null,"Lanyon ID":87847,"Internal Hotel Code":"WLGFS","City Latitude":-41.286461,"City Longitude":174.77623,"Hotel Latitude":-41.28,"Hotel Longitude":174.78,"Client Field 1":"EAST","Client Field 2":null,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"FABCDE","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Australia","Agency Code":"X8","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Australia","Ticketing Country":"Australia","Ticketing Region":"South Pacific","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"IQ","Hotel Chain Name":"Scanres Hospitality","Hotel Brand Name":"Sebel Suites","Property Name":"The Sebel Suites Auckland","Property Address 1":"85-89 Custom Street West","Property Address 2":null,"Property City":"Auckland-Intl","Property City Code":"AKL","Major City":"Auckland","Property State":null,"Postal Code":null,"Property Country Code":"NZ","Property Country":"New Zealand","Phone":6499784000,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"A2K","Room Type":"DLX 2 KING BEDS","Rate Category":"P30","Room Rate":322.5684,"Total Amount ":645.1368,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":1078.38,"Lowest Rate Available":307.208,"Source Currency Code":"USD","Transaction Currency Code":"USD","Currency Conversion Rate":0.0,"Amadeus ID":"AKLSEB","Worldspan ID":"SEAKL","Sabre ID":49803,"Apollo ID":24071,"HRS ID":null,"Lanyon ID":14119,"Internal Hotel Code":8759,"City Latitude":-37.00347,"City Longitude":174.78612,"Hotel Latitude":-36.8435287,"Hotel Longitude":174.7631531,"Client Field 1":"EAST","Client Field 2":46282671.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"GABCDE","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Australia","Agency Code":"X8","Booking Source":"Online","Local Hotel Reason Code Description":"Customer Preferred Hotel Booked","Global Hotel Reason Code Description":"Preferred Hotel Booked","Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Australia","Ticketing Country":"Australia","Ticketing Region":"South Pacific","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"MO","Hotel Chain Name":"Mandarin Oriental","Hotel Brand Name":"Mandarin Oriental","Property Name":"Mandarin Oriental Singapore","Property Address 1":"5 Raffles Avenue","Property Address 2":null,"Property City":"Singapore","Property City Code":"SIN","Major City":"Singapore","Property State":null,"Postal Code":"039797","Property Country Code":"SG","Property Country":"Singapore","Phone":6563380066,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F1K","Room Type":"1 KING BED               ","Rate Category":"BHD","Room Rate":240.4122,"Total Amount ":480.8244,"Hotel Preferred Indicator":"Y","Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":726.143,"Lowest Rate Available":228.964,"Source Currency Code":"USD","Transaction Currency Code":"SGD","Currency Conversion Rate":0.69018,"Amadeus ID":"SIN666","Worldspan ID":1134,"Sabre ID":2323,"Apollo ID":7786,"HRS ID":null,"Lanyon ID":14199,"Internal Hotel Code":3398,"City Latitude":1.352083,"City Longitude":103.819839,"Hotel Latitude":1.290093,"Hotel Longitude":103.858443,"Client Field 1":"EAST","Client Field 2":50417839.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"IABCDE","Traveler":"McTesterson\\/TestLong","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":123456789,"Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"SI","Hotel Chain Name":"Sheraton Hotels","Hotel Brand Name":"Sheraton","Property Name":"Sheraton Rio Hotel and Resort","Property Address 1":"Avenida Niemeyer 121","Property Address 2":null,"Property City":"Rio De Janeiro","Property City Code":"SDU","Major City":"Rio de Janeiro","Property State":"RJ","Postal Code":"22450-220","Property Country Code":"BR","Property Country":"Brazil","Phone":552125291122,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"A1K","Room Type":"DELUXE 1 KING            ","Rate Category":"FI1","Room Rate":231.6143,"Total Amount ":463.2286,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":220.585,"Lowest Rate Available":220.585,"Source Currency Code":"USD","Transaction Currency Code":"BRL","Currency Conversion Rate":0.19911,"Amadeus ID":"RIO255","Worldspan ID":255,"Sabre ID":483,"Apollo ID":814,"HRS ID":null,"Lanyon ID":194,"Internal Hotel Code":255,"City Latitude":-22.906847,"City Longitude":-43.172897,"Hotel Latitude":-22.992126,"Hotel Longitude":-43.233476,"Client Field 1":"NORTH","Client Field 2":46154200.0,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"NIFBMJ","Traveler":"McTesterson\\/TestShortest","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"WH","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"W Hotels","Property Name":"W Santiago","Property Address 1":"Isidora Goyenechea 3000","Property Address 2":null,"Property City":"Las Condes","Property City Code":"SCL","Major City":"Santiago","Property State":null,"Postal Code":null,"Property Country Code":"CL","Property Country":"Chile","Phone":5627700000,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"D2S","Room Type":"D2S","Rate Category":null,"Room Rate":206.4412,"Total Amount ":412.8824,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":196.6107,"Lowest Rate Available":196.6107,"Source Currency Code":"USD","Transaction Currency Code":"CLP","Currency Conversion Rate":0.001171,"Amadeus ID":"SCL979","Worldspan ID":1979,"Sabre ID":110044,"Apollo ID":47129,"HRS ID":null,"Lanyon ID":64042,"Internal Hotel Code":1979,"City Latitude":-33.445995,"City Longitude":-70.667057,"Hotel Latitude":-33.41409,"Hotel Longitude":-70.59849,"Client Field 1":"NORTH","Client Field 2":46119778.0,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"PRKPJB","Traveler":"McTesterson\\/TestShortest","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"SI","Hotel Chain Name":"Sheraton Hotels","Hotel Brand Name":"Sheraton","Property Name":"Sheraton Rio Hotel and Resort","Property Address 1":"Avenida Niemeyer 121","Property Address 2":null,"Property City":"Rio De Janeiro","Property City Code":"SDU","Major City":"Rio de Janeiro","Property State":"RJ","Postal Code":"22450-220","Property Country Code":"BR","Property Country":"Brazil","Phone":552125291122,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"A1K","Room Type":"DELUXE 1 KING            ","Rate Category":"FI1","Room Rate":231.6143,"Total Amount ":463.2286,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":220.585,"Lowest Rate Available":220.585,"Source Currency Code":"USD","Transaction Currency Code":"BRL","Currency Conversion Rate":0.19911,"Amadeus ID":"RIO255","Worldspan ID":255,"Sabre ID":483,"Apollo ID":814,"HRS ID":null,"Lanyon ID":194,"Internal Hotel Code":255,"City Latitude":-22.906847,"City Longitude":-43.172897,"Hotel Latitude":-22.992126,"Hotel Longitude":-43.233476,"Client Field 1":"NORTH","Client Field 2":46066350.0,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"SHFZWH","Traveler":"McTesterson\\/TestShorter","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"SI","Hotel Chain Name":"Sheraton Hotels","Hotel Brand Name":"Sheraton","Property Name":"Sheraton Rio Hotel and Resort","Property Address 1":"Avenida Niemeyer 121","Property Address 2":null,"Property City":"Rio De Janeiro","Property City Code":"SDU","Major City":"Rio de Janeiro","Property State":"RJ","Postal Code":"22450-220","Property Country Code":"BR","Property Country":"Brazil","Phone":552125291122,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"A1K","Room Type":"DELUXE 1 KING            ","Rate Category":"FI1","Room Rate":184.9253,"Total Amount ":369.8506,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-25T00:00:00.000Z","Service Fees":null,"Highest Rate Available":176.1193,"Lowest Rate Available":176.1193,"Source Currency Code":"USD","Transaction Currency Code":"BRL","Currency Conversion Rate":0.19911,"Amadeus ID":"RIO255","Worldspan ID":255,"Sabre ID":483,"Apollo ID":814,"HRS ID":null,"Lanyon ID":194,"Internal Hotel Code":255,"City Latitude":-22.906847,"City Longitude":-43.172897,"Hotel Latitude":-22.992126,"Hotel Longitude":-43.233476,"Client Field 1":"NORTH","Client Field 2":46197715.0,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"KIWPRB","Traveler":"McTesterson\\/TestShorter","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"RT","Hotel Chain Name":"Accor Hospitality","Hotel Brand Name":"Mercure","Property Name":"Mercure Bh Lourdes","Property Address 1":"Avenida Do Contorno 7315","Property Address 2":null,"Property City":"Belo Horizonte","Property City Code":"PLU","Major City":"Belo Horizonte","Property State":"MG","Postal Code":"30110-110","Property Country Code":"BR","Property Country":"Brazil","Phone":553132984100,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"A1D","Room Type":"DELUXE 1 DOUBLE          ","Rate Category":"Z6I","Room Rate":253.544,"Total Amount ":507.088,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-15T00:00:00.000Z","Service Fees":null,"Highest Rate Available":241.4705,"Lowest Rate Available":241.4705,"Source Currency Code":"USD","Transaction Currency Code":"EUR","Currency Conversion Rate":0.19911,"Amadeus ID":"BHZMER","Worldspan ID":"MRBHZ","Sabre ID":60164,"Apollo ID":37830,"HRS ID":null,"Lanyon ID":86239,"Internal Hotel Code":3575,"City Latitude":-19.9258406,"City Longitude":-43.9432044,"Hotel Latitude":-19.937811,"Hotel Longitude":-43.944825,"Client Field 1":"NORTH","Client Field 2":46459044.0,"Client Field 3":"CORPORATE","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"DPBGHZ","Traveler":"McTesterson\\/TestAlphashort","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":123456789,"Agency Name":"Demo - BCD Brazil","Agency Code":"W4","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"Domestic","Traveler Country":"Brazil","Ticketing Country":"Brazil","Ticketing Region":"Latin America","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"SI","Hotel Chain Name":"Sheraton Hotels","Hotel Brand Name":"Sheraton","Property Name":"Sheraton Rio Hotel and Resort","Property Address 1":"Avenida Niemeyer 121","Property Address 2":null,"Property City":"Rio De Janeiro","Property City Code":"SDU","Major City":"Rio de Janeiro","Property State":"RJ","Postal Code":"22450-220","Property Country Code":"BR","Property Country":"Brazil","Phone":552125291122,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F2Q","Room Type":"2 QUEEN BEDS             ","Rate Category":"BC2","Room Rate":434.6975,"Total Amount ":869.395,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":413.9976,"Lowest Rate Available":413.9976,"Source Currency Code":"USD","Transaction Currency Code":"BRL","Currency Conversion Rate":0.19911,"Amadeus ID":"RIO255","Worldspan ID":255,"Sabre ID":483,"Apollo ID":814,"HRS ID":null,"Lanyon ID":194,"Internal Hotel Code":255,"City Latitude":-22.906847,"City Longitude":-43.172897,"Hotel Latitude":-22.992126,"Hotel Longitude":-43.233476,"Client Field 1":"NORTH","Client Field 2":90884.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/TestAlphashort","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Jin Jiang China","Agency Code":"X4","Booking Source":"Online","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"China","Ticketing Country":"China","Ticketing Region":"Asia","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"SI","Hotel Chain Name":"Sheraton Hotels","Hotel Brand Name":"Sheraton","Property Name":"Sheraton New York Times Square Hotel","Property Address 1":"Fixed Prop Address","Property Address 2":null,"Property City":"New York","Property City Code":"LGA","Major City":"New York","Property State":"NY","Postal Code":"10019","Property Country Code":"US","Property Country":"United States","Phone":12125811000,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"C1D","Room Type":"STANDARD 1 DOUBLE        ","Rate Category":"F10","Room Rate":462.9408,"Total Amount ":925.8816,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-12T00:00:00.000Z","Service Fees":null,"Highest Rate Available":1763.59,"Lowest Rate Available":440.896,"Source Currency Code":"USD","Transaction Currency Code":"EUR","Currency Conversion Rate":0.0,"Amadeus ID":"NYC421","Worldspan ID":421,"Sabre ID":668,"Apollo ID":705,"HRS ID":null,"Lanyon ID":8785,"Internal Hotel Code":421,"City Latitude":40.7648,"City Longitude":-73.9808,"Hotel Latitude":40.762836,"Hotel Longitude":-73.981989,"Client Field 1":"WEST","Client Field 2":41413311.0,"Client Field 3":"RESEARCH","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/Testcorrect","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Hong Kong","Agency Code":"W1","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Hong Kong","Ticketing Country":"Hong Kong","Ticketing Region":"Asia","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"WI","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Westin Hotels","Property Name":"The Westin Chosun Seoul","Property Address 1":"106 Sogong-ro, Jung-gu","Property Address 2":"Jung-Gu","Property City":"Seoul","Property City Code":"GMP","Major City":"Seoul","Property State":"SE","Postal Code":"100","Property Country Code":"KR","Property Country":"Republic of Korea","Phone":8227710500,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"C1D","Room Type":"STANDARD 1 DOUBLE        ","Rate Category":"F10","Room Rate":208.2308,"Total Amount ":416.4616,"Hotel Preferred Indicator":null,"Booking Date":"2019-05-13T00:00:00.000Z","Service Fees":null,"Highest Rate Available":198.315,"Lowest Rate Available":198.315,"Source Currency Code":"USD","Transaction Currency Code":"KRW","Currency Conversion Rate":0.0008,"Amadeus ID":"SELWES","Worldspan ID":"CHOSE","Sabre ID":51,"Apollo ID":55,"HRS ID":null,"Lanyon ID":83895,"Internal Hotel Code":1064,"City Latitude":37.547895,"City Longitude":126.941893,"Hotel Latitude":37.564429,"Hotel Longitude":126.980066,"Client Field 1":"EAST","Client Field 2":41413311.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/Testcorrect","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Hong Kong","Agency Code":"W1","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Hong Kong","Ticketing Country":"Hong Kong","Ticketing Region":"Asia","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"WI","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Westin Hotels","Property Name":"Westin Beijing Financial Street","Property Address 1":"9B Financial Street","Property Address 2":"Xicheng District","Property City":"Beijing","Property City Code":"PEK","Major City":"Beijing","Property State":"BJ","Postal Code":"100140","Property Country Code":"CN","Property Country":"China","Phone":861066068866,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"C1D","Room Type":"STANDARD 1 DOUBLE        ","Rate Category":"F10","Room Rate":198.6569,"Total Amount ":397.3138,"Hotel Preferred Indicator":null,"Booking Date":"2019-04-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":189.197,"Lowest Rate Available":189.197,"Source Currency Code":"USD","Transaction Currency Code":"CNY","Currency Conversion Rate":0.141,"Amadeus ID":"BJS704","Worldspan ID":1704,"Sabre ID":70228,"Apollo ID":80435,"HRS ID":null,"Lanyon ID":17381,"Internal Hotel Code":1704,"City Latitude":39.9288889,"City Longitude":116.3883333,"Hotel Latitude":39.916165,"Hotel Longitude":116.352992,"Client Field 1":"EAST","Client Field 2":41413311.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/TestNoYear","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Hong Kong","Agency Code":"W1","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Hong Kong","Ticketing Country":"Hong Kong","Ticketing Region":"Asia","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"WI","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Westin Hotels","Property Name":"The Westin Chosun Seoul","Property Address 1":"106 Sogong-ro, Jung-gu","Property Address 2":"Jung-Gu","Property City":"Seoul","Property City Code":"GMP","Major City":"Seoul","Property State":"SE","Postal Code":"100","Property Country Code":"KR","Property Country":"Republic of Korea","Phone":8227710500,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"CLS","Room Type":"DELUXE CLASSIC           ","Rate Category":"FID","Room Rate":208.2308,"Total Amount ":416.4616,"Hotel Preferred Indicator":null,"Booking Date":"2019-06-01T00:00:00.000Z","Service Fees":null,"Highest Rate Available":198.315,"Lowest Rate Available":198.315,"Source Currency Code":"USD","Transaction Currency Code":"KRW","Currency Conversion Rate":0.0008,"Amadeus ID":"SELWES","Worldspan ID":"CHOSE","Sabre ID":51,"Apollo ID":55,"HRS ID":null,"Lanyon ID":83895,"Internal Hotel Code":1064,"City Latitude":37.547895,"City Longitude":126.941893,"Hotel Latitude":37.564429,"Hotel Longitude":126.980066,"Client Field 1":"EAST","Client Field 2":41413311.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null},{"Client Name":"CloudVenture","Global Customer Number":9002,"Locator":"ABCDEF","Traveler":"McTesterson\\/TestNoYear","Invoice Date":"2019-12-01T00:00:00.000Z","Invoice Number":"W123456789","Agency Name":"Demo - BCD Hong Kong","Agency Code":"W1","Booking Source":"Agency","Local Hotel Reason Code Description":null,"Global Hotel Reason Code Description":null,"Refund Indicator":"N","Exchange Indicator":"N","Original Document Number":null,"Int Dom":"International","Traveler Country":"Hong Kong","Ticketing Country":"Hong Kong","Ticketing Region":"Asia","Check-In Date":"2019-12-03T00:00:00.000Z","Check-Out Date":"2019-12-05T00:00:00.000Z","Confirmation Number":"W123456789","Hotel Chain Code":"WI","Hotel Chain Name":"Marriott International Inc.","Hotel Brand Name":"Westin Hotels","Property Name":"The Westin Chosun Seoul","Property Address 1":"106 Sogong-ro, Jung-gu","Property Address 2":"Jung-Gu","Property City":"Seoul","Property City Code":"GMP","Major City":"Seoul","Property State":"SE","Postal Code":"100","Property Country Code":"KR","Property Country":"Republic of Korea","Phone":8227710500,"Number Of Rooms":1,"Number Of Nights":2,"Room Type Code":"F1K","Room Type":"1 KING BED               ","Rate Category":"BAR","Room Rate":208.2308,"Total Amount ":416.4616,"Hotel Preferred Indicator":null,"Booking Date":"2019-04-17T00:00:00.000Z","Service Fees":null,"Highest Rate Available":198.315,"Lowest Rate Available":198.315,"Source Currency Code":"USD","Transaction Currency Code":"KRW","Currency Conversion Rate":0.0008,"Amadeus ID":"SELWES","Worldspan ID":"CHOSE","Sabre ID":51,"Apollo ID":55,"HRS ID":null,"Lanyon ID":83895,"Internal Hotel Code":1064,"City Latitude":37.547895,"City Longitude":126.941893,"Hotel Latitude":37.564429,"Hotel Longitude":126.980066,"Client Field 1":"EAST","Client Field 2":41413311.0,"Client Field 3":"DRILLING","Client Field 4":null,"Client Field 5":null,"Client Field 6":null,"Client Field 7":null,"Client Field 8":null,"Client Field 9":null,"Client Field 10":null}]'
// })