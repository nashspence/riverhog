# stove0_review_sampler_protocol.SamplerRequest.canonical_windows

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-samplerreq-f295d942cb:2f2374d6c5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e9cc7787c4"></a>
- <a id="s-023ed3c42c"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-a07a780808"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-48f1ebd9a1"></a>`name`: `canonical_windows`
- <a id="s-4a7f82fe83"></a>`owner`: `stove0_review_sampler_protocol.SamplerRequest`
- <a id="s-b5a505c141"></a>`unit`: `member`

### Declared structure

- <a id="s-9ccfb24687"></a>`kind`: `"classmethod"`
- <a id="s-f0857940e3"></a>`signature`: `"\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](stove0-review-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-79f5617c41"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.SamplerRequest.canonical_windows`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eac02daf4cf2fdfbe9618ecea08b42c45bdeab7ac7e2524acf1dca65f7ab77f8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "canonical_windows",
  "owner": "stove0_review_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
