const { TestSet, TestRunner } = require('alsatian')
const { TapBark } = require('tap-bark')
const { join } = require('path')

function runReader() {
  const reader = TestSet.create()
  reader.addTestsFromFiles('./spec/reader.js')
  return new Promise((resolve, reject) => {
    const runner = new TestRunner()
    runner.outputStream
      .pipe(TapBark.create().getPipeable())
      .pipe(process.stdout);
    runner.run(reader)
      .then(r => {
        console.log(r)
        return resolve()
      })
      .catch(e => {
        console.warn(e)
        return reject(e)
      })

  })
}
function runWriter() {
  const writer = TestSet.create()
  writer.addTestsFromFiles('./spec/writer.js')
  return new Promise((resolve, reject) => {
    const runner = new TestRunner()
    runner.outputStream
      .pipe(TapBark.create().getPipeable())
      .pipe(process.stdout);
    runner.run(writer)
      .then(r => {
        console.log(r)
        return resolve()
      })
      .catch(e => {
        console.warn(e)
        return reject(e)
      })
  })
}

async function start() {
  await runWriter()
  await runReader()
}

start()
