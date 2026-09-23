# review0_target_contracts.REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-9a241b0522:8404f02d90 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2ebafb699"></a>
- <a id="s-703b41d110"></a>`distribution`: `review0-target-contracts`
- <a id="s-9a924252e0"></a>`module`: `review0_target_contracts`
- <a id="s-8e055f95ee"></a>`name`: `REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS`
- <a id="s-aa1c1d54d3"></a>`unit`: `export`

### Declared structure

- <a id="s-50a5b33d9b"></a>`kind`: `"object"`
- <a id="s-00d5f98fc9"></a>`type`: `"stove0_target_protocol.conformance.SemanticIntentConformanceVectors"`

## Governing policies

- <a id="pa-5ea7087f49"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19811af3a9523bb49a32beb148c2355403f35059cbd909a2b43c13c482fd27cc -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS",
  "unit": "export"
}
```

</details>
