const redis = require("redis");
const async = require("async");
const config = require("config");
const debug = require("debug")("copy-redis");
const KEYSPACE_SECTION_NAME = 'Keyspace';
const ALL_KEYS = "*";
const DEFAULT_RESTORE_TTL = 0;


function connect(options) {
  return redis.createClient(options);
}
function type(client, key, next) {
  client.type(key, next);
}
function fetchOpFnk(client, key, next) {
  client.dump(key,next);
}
function setOpFnk(client, key, serializedData, next, override = false) {
  if (override) {
    client.del(key,(deleteErr, deleteResult) => {
      if (deleteErr) {
        next(deleteErr)
      } else {
        client.restore(key, DEFAULT_RESTORE_TTL, serializedData, next);
      }
    });
  } else {
    client.restore(key, serializedData, 0,next);
  }
}
function copyDb(sourceClient, targetClient, dbIndex, next, limit = 10, override = false) {
  sourceClient.select(dbIndex, (selectErr, selectResult) => {
    if (selectErr) {
      throw selectErr;
    } else {
      sourceClient.keys(ALL_KEYS, (keysErr, keys) => {
        debug(keys)
        async.mapLimit(keys, limit, (key) => {
          fetchOpFnk(sourceClient, key,(e, result) => {
            if (e) {
              throw e
            } else {
              setOpFnk(targetClient, key, result, (setOpError,setOpResult) => {
                if (setOpError) {
                  throw setOpError
                } else {
                  return setOpResult
                }
              }, override)
            }
          });
        })
      })

    }
  }, next)
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
  copyDb,
  getFilledDbs,
  executeCopy,
  config
}
