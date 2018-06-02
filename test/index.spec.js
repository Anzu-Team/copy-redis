import { expect } from 'chai'
const module = require("../index");
const debug = require("debug")("copy-redis-test");

describe('copy-redis module', function() {
  it('should return object', function() {
    expect(module).to.be.an.instanceOf(Object);
  });
  it('should have executeCopy function property', function() {
    expect(module.executeCopy).to.be.an.instanceOf(Function);
  });
  it('should have config object property', function() {
    expect(module.config).to.be.an.instanceOf(Object);
  });
  describe('config object should have', function() {
    it('an source options object property', function() {
      expect(module.config.source).to.be.an.instanceOf(Object);
    });
    it('an target options object property', function() {
      expect(module.config.target).to.be.an.instanceOf(Object);
    });
  });

  describe('connect function', function() {
    it('should return a redis client', function() {
      const source_client = module.connect(module.config.source);
      expect(source_client).to.be.an.instanceOf(Object);
    });
  });
  describe('getFilledDbs function', function() {
    it('should have getFilledDbs function that return an array of filled db indexes', function(done) {
      const source_client = module.connect(module.config.source);
      module.getFilledDbs(source_client, (err, result) => {
        expect(err).to.be.null;
        expect(result).to.be.an('array');
        done();
      })
    });
  });
  //xdescribe('executeCopy should handle an copy redis dbs data proccess from source to target based on options.', function() {

  //});
});

