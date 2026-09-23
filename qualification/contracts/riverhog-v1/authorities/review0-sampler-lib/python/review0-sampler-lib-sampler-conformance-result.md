# review0_sampler_lib.SAMPLER_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-sampler-conformance-result:e335522f74 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e564b1602"></a>
- <a id="s-91b77a45e5"></a>`distribution`: `review0-sampler-lib`
- <a id="s-f9427b24db"></a>`module`: `review0_sampler_lib`
- <a id="s-544c0095e1"></a>`name`: `SAMPLER_CONFORMANCE_RESULT`
- <a id="s-bb02a3c2c2"></a>`unit`: `export`

### Declared structure

- <a id="s-b0620b0cb9"></a>`kind`: `"constant"`
- <a id="s-3ad4f59542"></a>`value`: `"review0-sampler-conformance-result/v1"`

## Governing policies

- <a id="pa-fe20d8ebc9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SAMPLER_CONFORMANCE_RESULT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf144de8d1a29c45d5ace65440cfbe3b6e6d637a75b0eb897452faf0cb884b15 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "review0-sampler-conformance-result/v1"
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SAMPLER_CONFORMANCE_RESULT",
  "unit": "export"
}
```

</details>
