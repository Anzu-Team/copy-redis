const redis = require("redis");
const async = require("async");
const config = require("config");
const debug = require("debug")("copy-redis");
const KEYSPACE_SECTION_NAME = 'Keyspace';

function connect(options) {
  return redis.createClient(options);
}

function getFilledDbs(client,next) {
  client.info((err, result) => {
    let dbsList = [];
    let sections = result.split("#")
    for (var i = 0, len = sections.length; i < len; i++) {
      let line = sections[i];
      let sectionName = line.substring(1,9);
      if (sectionName == KEYSPACE_SECTION_NAME) {
        let keyspaceLines = line.split('\r\n');
        keyspaceLines.pop();
        keyspaceLines.shift();
        keyspaceLines.map(function (element, index) {
          let dbIndex = element.split(",")[0].split(":")[0].substring(2);
          dbIndex = parseInt(dbIndex);
          if (isNaN(dbIndex)) {
            throw new Error('Incorrect parsing of keyspace section lines.')
          } else {
            dbsList.push(dbIndex)
          }
        });
      }
    }
    next(err,dbsList)
  })
}
function executeCopy(options, next) {
  if (options.source) {
    const client = connect(options.source);
    getFilledDbs(client, (err, result) => {
      next(err, result);
    })
  } else {
    throw new Error("Redis source connection options are not defined")
  }
}

module.exports = {
  connect,
  getFilledDbs,
  executeCopy,
  config
}
