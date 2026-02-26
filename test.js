// Adapted from https://github.com/apollographql/graphql-subscriptions/blob/master/src/test/tests.ts
const { isAsyncIterable } = require("iterall");
const { Client } = require("pg");
const assert = require("assert");
const sinon = require("sinon");

const { PostgresPubSub } = require("./dist/cjs");

describe("PostgresPubSub", () => {
  let client;

  beforeEach(async () => {
    client = new Client();
    await client.connect();
  });

  it("PostgresPubSub can subscribe when instantiated without a client", function (done) {
    const ps = new PostgresPubSub({ usePayloadTable: false });
    ps.subscribe("a", (payload) => {
      assert.strictEqual(payload, "test");
      done();
    }).then(() => {
      const succeed = ps.publish("a", "test");
      assert.strictEqual(succeed, true);
    });
  });

  it("PostgresPubSub can subscribe and is called when events happen", function (done) {
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    ps.subscribe("a", (payload) => {
      assert.strictEqual(payload, "test");
      done();
    }).then(() => {
      const succeed = ps.publish("a", "test");
      assert.strictEqual(succeed, true);
    });
  });

  it("PostgresPubSub can subscribe when instantiated with connection options but without a client", function (done) {
    const ps = new PostgresPubSub({
      connectionString: process.env.DATABASE_URL,
      usePayloadTable: false
    });
    ps.subscribe("a", (payload) => {
      assert.strictEqual(payload, "test");
      done();
    }).then(() => {
      const succeed = ps.publish("a", "test");
      assert.strictEqual(succeed, true);
    });
  });

  it("should send notification event after calling publish", (done) => {
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    client.on("notification", ({ payload }) => {
      assert.strictEqual(payload, "test");
      done();
    });
    ps.subscribe("a", (payload) => {
      assert.strictEqual(payload, "test");
    }).then(() => {
      const succeed = ps.publish("a", "test");
      assert.strictEqual(succeed, true);
    });
  });

  it("PostgresPubSub can unsubscribe", function (done) {
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    ps.subscribe("a", (payload) => {
      assert.fail("Should not reach this point");
    }).then((subId) => {
      ps.unsubscribe(subId);
      const succeed = ps.publish("a", "test");
      assert.strictEqual(succeed, true); // True because publish success is not
      // indicated by trigger having subscriptions
      done(); // works because pubsub is synchronous
    });
  });

  it("Should emit error when payload exceeds Postgres 8000 character limit", (done) => {
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    ps.subscribe("a", () => {
      assert.fail("Should not reach this point");
      done();
    });
    ps.subscribe("error", (err) => {
      assert.strictEqual(err.message, "payload string too long");
      done();
    }).then(() => {
      const succeed = ps.publish("a", "a".repeat(9000));
      assert.strictEqual(succeed, true);
    });
  });

  it("AsyncIterator should expose valid asyncIterator for a specific event", () => {
    const eventName = "test";
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    const iterator = ps.asyncIterator(eventName);
    assert.notStrictEqual(iterator, undefined);
    assert.strictEqual(isAsyncIterable(iterator), true);
  });

  it("AsyncIterator should trigger event on asyncIterator when published", (done) => {
    const eventName = "test";
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    const iterator = ps.asyncIterator(eventName);

    iterator.next().then((result) => {
      assert.notStrictEqual(result, undefined);
      assert.notStrictEqual(result.value, undefined);
      assert.strictEqual(result.done, false);
      done();
    });

    ps.publish(eventName, { test: true });
  });

  it("AsyncIterator should not trigger event on asyncIterator when publishing other event", (done) => {
    const eventName = "test2";
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    const iterator = ps.asyncIterator("test");
    const spy = sinon.spy();

    iterator.next().then(spy);
    ps.publish(eventName, { test: true });
    assert(spy.notCalled);
    done();
  });

  it("AsyncIterator should register to multiple events", (done) => {
    const eventName = "test2";
    const ps = new PostgresPubSub({ client, usePayloadTable: false });
    const iterator = ps.asyncIterator(["test", "test2"]);
    const spy = sinon.spy();

    iterator.next().then(() => {
      spy();
      assert.strictEqual(spy.callCount, 1);
      done();
    });
    ps.publish(eventName, { test: true });
  });

  it("AsyncIterator transforms messages using commonMessageHandler", (done) => {
    const eventName = "test";
    const commonMessageHandler = (message) => ({ transformed: message });
    const ps = new PostgresPubSub({ client, usePayloadTable: false, commonMessageHandler });
    const iterator = ps.asyncIterator(eventName);

    iterator.next().then((result) => {
      assert.notStrictEqual(result, undefined);
      assert.deepStrictEqual(result.value, { transformed: { test: true } });
      assert.strictEqual(result.done, false);
      done();
    });

    ps.publish(eventName, { test: true });
  });

  it("PostgresPubSub transforms messages using commonMessageHandler", function (done) {
    const commonMessageHandler = (message) => ({ transformed: message });
    const ps = new PostgresPubSub({ client, usePayloadTable: false, commonMessageHandler });
    ps.subscribe("transform", (payload) => {
      assert.deepStrictEqual(payload, { transformed: { test: true } });
      done();
    }).then(() => {
      const succeed = ps.publish("transform", { test: true });
      assert.strictEqual(succeed, true);
    });
  });

  // This test does not clean up after it ends. It breaks the test that follows after it.
  // It won't break any tests if it's the last. https://imgflip.com/i/2lmlgm
  // TODO: Fix it properly
  it("AsyncIterator should not trigger event on asyncIterator already returned", (done) => {
    (async () => {
      const eventName = "test";
      const ps = new PostgresPubSub({ client, usePayloadTable: false });
      const iterator = ps.asyncIterator(eventName);

      const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

      iterator.next().then((result) => {
        assert.notStrictEqual(result, undefined);
        assert.notStrictEqual(result.value, undefined);
        assert.strictEqual(result.done, false);
      });

      ps.publish(eventName, { test: true });

      await delay(0);

      iterator.next().then((result) => {
        assert.notStrictEqual(result, undefined);
        assert.strictEqual(result.value, undefined);
        assert.strictEqual(result.done, true);
        done();
      });

      await delay(0);

      iterator.return?.();

      ps.publish(eventName, { test: true });
    })();
  });
});
