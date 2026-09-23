# review0_sampler_protocol.SamplerRequest.references_exact_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest-r-497369e3e7:631b6720b5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-89d226f8bc"></a>
- <a id="s-706edca9a8"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-1c5dcb6c74"></a>`module`: `review0_sampler_protocol`
- <a id="s-578da0a022"></a>`name`: `references_exact_inputs`
- <a id="s-9474dbefd0"></a>`owner`: `review0_sampler_protocol.SamplerRequest`
- <a id="s-fef2d1f8c2"></a>`unit`: `member`

### Declared structure

- <a id="s-2ad07a27bd"></a>`kind`: `"method"`
- <a id="s-07f7d649ee"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](review0-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-4797731c5a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest.references_exact_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0a01fbe2cdfe5ae5e0a9902724dff8b3fbbdd745740af3bf5e8cdff3ec5fee2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "references_exact_inputs",
  "owner": "review0_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
