import test from "node:test";
import assert from "node:assert/strict";
import { CircuitBreaker } from "../dist/circuit-breaker.js";

test("после серии ошибок пропускает только одну пробу, затем восстанавливается", () => {
  const breaker = new CircuitBreaker();
  assert.equal(breaker.allow(0), true);
  breaker.failure(0);
  breaker.failure(0);
  breaker.failure(0);
  assert.equal(breaker.allow(4999), false);
  assert.equal(breaker.allow(5000), true);
  assert.equal(breaker.allow(5001), false);
  breaker.failure(5001);
  assert.equal(breaker.allow(10000), false);
  assert.equal(breaker.allow(10001), true);
  breaker.success();
  assert.equal(breaker.allow(10002), true);
  assert.equal(breaker.allow(10002), true);
});
