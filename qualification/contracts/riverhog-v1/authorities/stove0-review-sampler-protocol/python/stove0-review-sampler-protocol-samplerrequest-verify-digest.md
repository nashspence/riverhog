# stove0_review_sampler_protocol.SamplerRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerreq-345407ae9c:94e6c99cee -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbe58c8a40"></a>
- <a id="s-771432fd88"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-1e287a464b"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-5b3e59d28e"></a>`name`: `verify_digest`
- <a id="s-b73b96049c"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequest`
- <a id="s-2da4325c1b"></a>`unit`: `member`

### Declared structure

- <a id="s-0c9e423050"></a>`kind`: `"method"`
- <a id="s-1385b6c28e"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](stove0-review-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-f2c88fb762"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources/authorities.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 218a4dd7d4219664ef9220bd3b0bf69fa3db57142607d44e8e676468f79c4c77 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "verify_digest",
  "owner": "stove0_review_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
