// NON-AUTHORITATIVE #897 test oracle, not a production parser or duplicate-key validator.
// JSON.stringify provides ECMAScript number/string serialization. Emit sorted names
// directly: inserting numeric-looking keys into an object would reorder them again.
import { readFileSync } from 'node:fs';
function canonical(value) {
  if (value === null || typeof value !== 'object') {
    if (typeof value === 'number' && !Number.isFinite(value)) throw Error('nonfinite');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}
const request = JSON.parse(readFileSync(0, 'utf8'));
const response = {
  json: request.json.map(raw => canonical(JSON.parse(raw))),
  binary64: request.binary64.map(hex => {
    const value = Buffer.from(hex, 'hex').readDoubleBE();
    return Number.isFinite(value) ? canonical(value) : null;
  }),
  scalars: request.scalars.map(({ pattern, value }) => typeof value === 'string' && new RegExp(pattern, 'u').test(value))
};
process.stdout.write(JSON.stringify(response));
