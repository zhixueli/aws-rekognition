process.env.PATH = process.env.PATH + ':' + process.env['LAMBDA_TASK_ROOT']

const AWS = require('aws-sdk')
const { spawn, spawnSync } = require('child_process')
const { createReadStream, createWriteStream } = require('fs')

const s3 = new AWS.S3()
const ffprobePath = '/opt/nodejs/node_modules/ffprobe'
const ffmpegPath = '/opt/nodejs/node_modules/ffmpeg'
const allowedTypes = ['mov', 'mpg', 'mpeg', 'mp4', 'wmv', 'avi', 'webm']
const width = 640
const height = 360
const rek = new AWS.Rekognition()
const tran = new AWS.Translate()

module.exports.handler = async (event, context) => {
  console.log(`event:  ${JSON.stringify(event)}`)
  const srcKey = decodeURIComponent(event.Records[0].s3.object.key).replace(/\+/g, ' ')
  console.log(`srckey:  ${srcKey}`)
  //const srcKey = 'input/rekvideo.mp4'
  const bucket = event.Records[0].s3.bucket.name
  console.log(`bucket:  ${bucket}`)
  //const bucket = 'sunbiao-tokyo-ok-rek'
  const target = s3.getSignedUrl('getObject', { Bucket: bucket, Key: srcKey, Expires: 1000 })
  let fileType = srcKey.match(/\.\w+$/)
  let resultJson = {}
  resultJson.Labels = []

  if (!fileType) {
    throw new Error(`invalid file type found for key: ${srcKey}`)
  }

  fileType = fileType[0].slice(1)

  if (allowedTypes.indexOf(fileType) === -1) {
    throw new Error(`filetype: ${fileType} is not an allowed type`)
  }

  function createImage(seek) {
    return new Promise((resolve, reject) => {
      let tmpFile = createWriteStream(`/tmp/screenshot.jpg`)
      const ffmpeg = spawn(ffmpegPath, [
        '-ss',
        seek,
        '-i',
        target,
        '-vf',
        `thumbnail,scale=${width}:${height}`,
        '-qscale:v',
        '2',
        '-frames:v',
        '1',
        '-f',
        'image2',
        '-c:v',
        'mjpeg',
        'pipe:1'
      ])

      ffmpeg.stdout.pipe(tmpFile)

      ffmpeg.on('close', function(code) {
        tmpFile.end()
        resolve()
      })

      ffmpeg.on('error', function(err) {
        console.log(err)
        reject()
      })
    })
  }

  function uploadToS3(key) {
    return new Promise((resolve, reject) => {
      let tmpFile = createReadStream(`/tmp/screenshot.jpg`)
      var params = {
        Bucket: bucket,
        Key: key,
        Body: tmpFile,
        ContentType: `image/jpg`
      }

      s3.upload(params, function(err, data) {
        if (err) {
          console.log(err)
          reject()
        }
        console.log(`successful upload to ${bucket}/${key}`)
        resolve()
      })
    })
  }
  
  function uploadResultToS3(key) {
    return new Promise((resolve, reject) => {
      var params = {
        Bucket: "rekognition-video-console-demo-iad-zhixue-t3f3bmzmt77vo00gsesb",
        Key: key,
        Body: JSON.stringify(resultJson),
        ContentType: `text/json`
      }

      s3.upload(params, function(err, data) {
        if (err) {
          console.log(err)
          reject()
        }
        console.log(`successful upload to rekognition-video-console-demo-iad-zhixue-t3f3bmzmt77vo00gsesb/${key}`)
        resolve()
      })
    })
  }
  
  function detectImageLabels(s3bucket, s3key) {
    return new Promise((resolve, reject) => {
      
      var params = {
        Image: {
         S3Object: {
          Bucket: s3bucket, 
          Name: s3key
         }
        }, 
        MaxLabels: 15, 
        MinConfidence: 90
       } 
       
       rek.detectLabels(params, function(err, data) {
         if (err) {
           console.log(err, err.stack)
           reject()
         }
         else {
           console.log(`detect labels from image ${s3bucket}/${s3key} successfully`)
           
           data.Labels.forEach(label => {
             let obj = {}
             obj.Timestamp = offset * 1000
             obj.Label = label
             resultJson.Labels.push(obj)
           })
           
           console.log(JSON.stringify(resultJson))
           resolve()
         }
       })
    })
  }
  
  function translate(txt) {
    return new Promise((resolve, reject) => {
      
      var params = {
        SourceLanguageCode: 'en',
        TargetLanguageCode: 'zh',
        Text: txt
      };
    
      tran.translateText(params, function (err, data) {
        if (err) {
          console.log(err, err.stack);
          reject()
        }
        else {
          zhTxt = data['TranslatedText']
          console.log(zhTxt);
          resolve()
        }
      });
    })
  }

  const ffprobe = spawnSync(ffprobePath, [
    '-v',
    'error',
    '-show_entries',
    'format=duration',
    '-of',
    'default=nw=1:nk=1',
    target
  ])

  const duration = Math.ceil(ffprobe.stdout.toString())
  const interval = 6
  
  let offset = 1
  
  while(offset < duration) {
    console.log(`capture frame on ${offset} second`)
    let dstkey = srcKey.replace(/\.\w+$/, `-${offset}s.jpg`).replace('input', 'output')
    await createImage(offset)
    await uploadToS3(dstkey)
    await detectImageLabels(bucket, dstkey)
    offset += interval
  }
  
  let enTxtList = []
  let zhTxtList = []
  let zhTxt = ""
  let resultJsonStr = JSON.stringify(resultJson)
  
  resultJson.Labels.forEach(label => {
    var txt = label.Label.Name
    if (enTxtList.indexOf(txt) < 0) {
      enTxtList.push(txt)
    }
  })
  
  let enText = enTxtList.toString().replace(new RegExp(',', 'g'), '\n').toLowerCase()
  console.log(enText)
  await translate(enText)
  
  zhTxtList = zhTxt.split('\n')
  
  let count = 0
  enTxtList.forEach(txt => {
    var txt2 = zhTxtList[count]
    console.log(`replace ${txt} with ${txt2}`)
    resultJsonStr = resultJsonStr.replace(new RegExp(`"${txt}"`, 'g'), `"${txt2}"`)
    //console.log(resultJsonStr)
    count ++
  })
  
  console.log(resultJsonStr)
  resultJson = JSON.parse(resultJsonStr)
  
  let fileName = srcKey.split('/').pop()
  let resultkey = fileName.replace(/\.\w+$/, `.json`)
  await uploadResultToS3(resultkey)

  console.log(`processed ${bucket}/${srcKey} successfully`)
  
  return null
}