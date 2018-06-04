const copyRedis = require("./index");
const debug = require("debug")("copy-redis-execute-copy");

copyRedis.executeCopy(copyRedis.config, (err, result) => {
  debug(err, result);
  process.exit();
});
