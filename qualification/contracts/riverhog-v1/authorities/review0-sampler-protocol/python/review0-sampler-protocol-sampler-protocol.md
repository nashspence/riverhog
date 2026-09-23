# review0_sampler_protocol.SAMPLER_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-sampler-protocol:b43c21772e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4497f38b9"></a>
- <a id="s-eaad6ab9b4"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-a90300153a"></a>`module`: `review0_sampler_protocol`
- <a id="s-0126db78b9"></a>`name`: `SAMPLER_PROTOCOL`
- <a id="s-0c79fac27e"></a>`unit`: `export`

### Declared structure

- <a id="s-b149dae610"></a>`kind`: `"constant"`
- <a id="s-68c78302ef"></a>`value`: `"review0-sampler/v1"`

## Governing policies

- <a id="pa-aa40e3228d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SAMPLER_PROTOCOL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: adf673b256635c75bb84a5183913372bd20d45c4450b3c8ce67053e2b7d3f3d0 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "review0-sampler/v1"
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "SAMPLER_PROTOCOL",
  "unit": "export"
}
```

</details>
