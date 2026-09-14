# stove0_target_support.canonical_json_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-canonical-json-sha256:735789f268 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81fb773cf5"></a>
- <a id="s-5244e25f48"></a>`distribution`: `stove0-target-support`
- <a id="s-c3ad03e725"></a>`module`: `stove0_target_support`
- <a id="s-6ffc598148"></a>`name`: `canonical_json_sha256`
- <a id="s-cb21999288"></a>`unit`: `export`

### Declared structure

- <a id="s-01bddf0c5d"></a>`kind`: `"function"`
- <a id="s-8ee9584920"></a>`signature`: `"\"(value: 'object') -> 'str'\""`

## Governing policies

- <a id="pa-cce202d7dc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.canonical_json_sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 550cfb7851c7e13f9bf3342e879a942e5686e7dbf0a438f530442eada856d66f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'str'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_json_sha256",
  "unit": "export"
}
```
