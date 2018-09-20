# NORC
This project provides a nodejs binding to apache orc core c++.
<mark>This project is in very early stages of development</mark>

### Run on AWS Lambda
NORC has a companion package `norc-aws` for execution in aws lambda.
The binary provided in the `norc-aws` package is built on ORC master on Centos7.


Build notes:

- use devtoolset-7
- install snappy-devel, protobuf-devel and lz4-devel 
- disable java, libhdsfpp, tests, vendor libs, and tools
- provide cmake variables protobuf_home,snappy_home, and lz4_home, these should all be /usr
- set required flags; cmake_cxx_flags="-fPIC"
- set linker flags: LDFLAGS=-Wl,-rpath=/var/task/node_modules/norc-aws/lib

To use norc in lambda use one of the pre-built release binaries. Install into an npm based project
with `npm i -S https://github.com/nPilots/norc/releases/download/{version}/norc-{version}-linux-aws-x64.tar.gz norc-aws`


### Usage

Convert a csv file to an orc file.
```typescript
import {norc, DataType} from '@npilots/norc'
import {fromPath} from 'fast-csv'
function convert(csv:string, output:string): void {
    const writer = new norc.Writer(output)
    const schema = {x: DataType.SMALLINT, y: DataType.SMALLINT}
    writer.schema(schema)
    fromPath(csv,{ headers: true })
        .on('data', chunk => {
            writer.add({x: chunk.x, y: chunk.y})
        })
        .on('end', () => {
            writer.close()
            // do something with .orc file
        })
}
```

Read a file
```typescript
import {norc: {Reader}} from '@npilot/norc'
// Read as iterator
const reader = new Reader('/path/to/orcfile')
reader.read({columns: ['id', 'foobar']}, (err, it) => {
    let i = it.next()
    while(!i.done) {
        // do something with i.value
        i = it.next()
    }
})
// Read and listen .on('data')
reader.on('data', chunk => {
    chunk = JSON.parse(chunk)
    // do something with chunk
})
reader.on('end', () => {
    // end of contents
})
reader.read()
```
