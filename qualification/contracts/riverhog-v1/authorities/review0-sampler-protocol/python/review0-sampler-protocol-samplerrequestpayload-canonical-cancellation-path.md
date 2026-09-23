# review0_sampler_protocol.SamplerRequestPayload.canonical_cancellation_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequestpa-6ae6fa3a9d:33ff5d3ad1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-173a08a17e"></a>
- <a id="s-fc8cb77dc3"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-4a1cd0ca1d"></a>`module`: `review0_sampler_protocol`
- <a id="s-39440cf514"></a>`name`: `canonical_cancellation_path`
- <a id="s-0bca5ffa3a"></a>`owner`: `review0_sampler_protocol.SamplerRequestPayload`
- <a id="s-f95b14efd1"></a>`unit`: `member`

### Declared structure

- <a id="s-0b2d6c44de"></a>`kind`: `"classmethod"`
- <a id="s-a6450de460"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequestPayload](review0-sampler-protocol-samplerrequestpayload.md)

## Governing policies

- <a id="pa-a975e17deb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequestPayload.canonical_cancellation_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27bc66b4faeffe29446ed1ab550af3403042ad923af155d95a8fda7f1df629a2 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_cancellation_path",
  "owner": "review0_sampler_protocol.SamplerRequestPayload",
  "unit": "member"
}
```

</details>
