import { expect } from "chai";
const module = require("../index");
const debug = require("debug")("copy-redis-test");
const async = require("async");
const SP = '_';

describe("copy-redis module", function() {
  before((done) => {
      const source_client = module.connect(module.config.source);
      source_client.select(0,(e1,r1) => {
        async.mapSeries([...Array(5).keys()],function (indx,clbk) {
          debug(indx);
          let key = 'copy-redis-test' + SP + indx;
          source_client.set(key, indx, (e1, r1) => {
            clbk(null, [indx,key,indx,e1,r1]);
          });
        }, (err, res0) => {
          source_client.select(5,(e1,r1) => {
            async.mapSeries([...Array(5).keys()],function (indx,clbk) {
              debug(indx);
              let key = 'copy-redis-test' + SP + indx;
              source_client.set(key, indx, (e1, r1) => {
                clbk(null, [indx,key,indx,e1,r1]);
              });
            }, (err, res5) => {
              debug(err,[res0,res5]);
              done();
            })
          })
        })
      })
  })
  it("should return object", function() {
    expect(module).to.be.an.instanceOf(Object);
  });
  it("should have executeCopy function property", function() {
    expect(module.executeCopy).to.be.an.instanceOf(Function);
  });
  it("should have copyDb function property", function() {
    expect(module.copyDb).to.be.an.instanceOf(Function);
  });
  it("should have config object property", function() {
    expect(module.config).to.be.an.instanceOf(Object);
  });
  describe("config object should have", function() {
    it("an source options object property", function() {
      expect(module.config.source).to.be.an.instanceOf(Object);
    });
    it("an target options object property", function() {
      expect(module.config.target).to.be.an.instanceOf(Object);
    });
  });

  describe("connect function", function() {
    it("should return a redis client", function() {
      const source_client = module.connect(module.config.source);
      expect(source_client).to.be.an.instanceOf(Object);
    });
  });
  describe("getFilledDbs function", function() {
    it("should have getFilledDbs function that return an array of filled db indexes", function(done) {
      const source_client = module.connect(module.config.source);
      module.getFilledDbs(source_client, (err, result) => {
        expect(err).to.be.null;
        expect(result).to.be.an("array");
        done();
      });
    });
  });
  describe("copyDb function", function() {
    it("should return err or result of copy operation between redis servers, \
limited to batches of 10 async calls by default.", function(done) {
      const source_client = module.connect(module.config.source);
      const target_client = module.connect(module.config.target);
      const dbIndex = 0;
      module.copyDb(source_client, target_client, dbIndex, (err, result) => {
        expect(result).to.be.an("array");
        expect(err).to.be.null;
        done()
      });
    });
  });
  describe('executeCopy function', function() {
    it('should copy keys between instances.', function(done) {
      module.executeCopy(module.config, (err, result) => {
        debug(err, result);
        done()
      })
    });

  });
});
