# stove0_target_support.canonical_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-canonical-json-bytes:512117de6d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c15662d80"></a>
- <a id="s-d9524cff82"></a>`distribution`: `stove0-target-support`
- <a id="s-7f23f4f1b9"></a>`module`: `stove0_target_support`
- <a id="s-efdf09f1cc"></a>`name`: `canonical_json_bytes`
- <a id="s-fb7f89a6ff"></a>`unit`: `export`

### Declared structure

- <a id="s-331ccdc62c"></a>`kind`: `"function"`
- <a id="s-ae3180ad23"></a>`signature`: `"\"(value: 'object') -> 'bytes'\""`

## Governing policies

- <a id="pa-92f4577e92"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.canonical_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f271ea6c187eefd3badac2c77cb3c51e60ee2cd1f0679359f73e982af7f63f66 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'bytes'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_json_bytes",
  "unit": "export"
}
```
