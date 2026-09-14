# stove0_target_support.TARGET_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-target-conformance-result:7475b7084a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2537f75d3"></a>
- <a id="s-781b33302e"></a>`distribution`: `stove0-target-support`
- <a id="s-143efd3987"></a>`module`: `stove0_target_support`
- <a id="s-e5fc5357ef"></a>`name`: `TARGET_CONFORMANCE_RESULT`
- <a id="s-05332050c9"></a>`unit`: `export`

### Declared structure

- <a id="s-6d81ed667a"></a>`kind`: `"constant"`
- <a id="s-8e84967fe7"></a>`value`: `"stove0-target-conformance-result/v1"`

## Governing policies

- <a id="pa-26b42b65b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TARGET_CONFORMANCE_RESULT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7d9a83598b373c663aae8b6a91ef8dfb29bb8a305242fa749b2237b4860e2401 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-target-conformance-result/v1"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TARGET_CONFORMANCE_RESULT",
  "unit": "export"
}
```
