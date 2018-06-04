const redis = require("redis");
const async = require("async");
const config = require("config");
const debug = require("debug")("copy-redis");
const KEYSPACE_SECTION_NAME = "Keyspace";
const ALL_KEYS = "*";
const DEFAULT_RESTORE_TTL = 0;

const types = {
  string: ["get", "set"],
  list: ["lrange", "rpush"]
};
function connect(options) {
  return redis.createClient(options);
}
function type(client, key, next) {
  client.type(key, next);
}
function copyKey(sourceClient, targetClient, dbIndex, key, next) {
  debug("copy key " + key + " of db"+dbIndex)
  type(sourceClient, key, (err, type) => {
    let getFnkName = types[type][0];
    let setFnkName = types[type][1];
    sourceClient.select(dbIndex, (selErr, sRes) => {
      if (selErr) {
        next(selErr);
      } else {
        sourceClient[getFnkName](key, (gErr, gRes) => {
          if (gErr) {
            next(gErr);
          } else {
            targetClient.select(dbIndex, (tErr,tSelRes) => {
              if (tErr) {
                next(tErr)
              } else {
                targetClient[setFnkName](key, gRes, (sErr, sRes) => {
                  if (sErr) {
                    next(sErr)
                  } else {
                    next(null,[key, gRes, sRes])
                  }
                });
              }
            });
          }
        });
      }
    });
  });
}
function setOpFnk(client, key, serializedData, next, override = false) {
  if (override) {
    client.del(key, (deleteErr, deleteResult) => {
      if (deleteErr) {
        next(deleteErr);
      } else {
        client.restore(key, DEFAULT_RESTORE_TTL, serializedData, next);
      }
    });
  } else {
    client.restore(key, serializedData, 0, next);
  }
}
function copyDb(sourceClient, targetClient, dbIndex, next, limit = 10) {
  sourceClient.select(
    dbIndex,
    (selectErr, selectResult) => {
      if (selectErr) {
        next(selectErr);
      } else {
        sourceClient.keys(ALL_KEYS, (keysErr, keys) => {
          async.mapLimit(keys, limit, ( key, clbk ) => {
            copyKey(sourceClient, targetClient, dbIndex, key, (e, result) => {
              if (e) {
                clbk(e);
              } else {
                clbk(null, result);
              }
            });
          }, next);
        });
      }
    }
  );
}

function getFilledDbs(client, next) {
  client.info((err, result) => {
    let dbsList = [];
    let sections = result.split("#");
    for (var i = 0, len = sections.length; i < len; i++) {
      let line = sections[i];
      let sectionName = line.substring(1, 9);
      if (sectionName == KEYSPACE_SECTION_NAME) {
        let keyspaceLines = line.split("\r\n");
        keyspaceLines.pop();
        keyspaceLines.shift();
        keyspaceLines.map(function(element, index) {
          let dbIndex = element
            .split(",")[0]
            .split(":")[0]
            .substring(2);
          dbIndex = parseInt(dbIndex);
          if (isNaN(dbIndex)) {
            throw new Error("Incorrect parsing of keyspace section lines.");
          } else {
            dbsList.push(dbIndex);
          }
        });
      }
    }
    debug("Found following dbs as filled", dbsList)
    next(err, dbsList);
  });
}

/**
 * executeCopy, will halt if any of key values failed to be copied.
 * db copy is executed using `async.series` methodology.
 * While `copyKey` is executed as parallel `async` calls limited to 10 keys, by default.
 * throws if options are not defined.
 *
 * @param options
 * @param next
 * @returns {Array}
 */

function executeCopy(options, next) {
  if (options.source) {
    const sourceClient = connect(options.source);
    const targetClient = connect(options.target);
    if (options.dbs) {
      if (typeof options.dbs == 'array') {
        async.mapSeries(options.dbs, (dbIndex,clbk) => {
          debug(dbIndex)
          copyDb(sourceClient, targetClient,dbIndex, clbk);
        }, next)
      } else {
        throw new Error("Passed db ist through `options.dbs` is not based on JS array date type");
      }
    } else {
      getFilledDbs(sourceClient, (err, dbs) => {
        async.mapSeries(dbs, (dbIndex,clbk) => {
          debug(dbIndex)
          copyDb(sourceClient, targetClient,dbIndex, clbk);
        }, next)
      });
    }
  } else {
    throw new Error("Redis source connection options are not defined");
  }
}

module.exports = {
  connect,
  copyDb,
  getFilledDbs,
  executeCopy,
  config
};
