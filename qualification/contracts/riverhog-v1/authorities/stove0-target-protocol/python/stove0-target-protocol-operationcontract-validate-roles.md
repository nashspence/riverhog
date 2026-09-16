# stove0_target_protocol.OperationContract.validate_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontract-5c0e2b94b9:8e0d281bb4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef64b84060"></a>
- <a id="s-88c63fea56"></a>`distribution`: `stove0-target-protocol`
- <a id="s-4a3f8ac0ee"></a>`module`: `stove0_target_protocol`
- <a id="s-4470e74ee8"></a>`name`: `validate_roles`
- <a id="s-1c3ea9865b"></a>`owner`: `stove0_target_protocol.OperationContract`
- <a id="s-b60dc0550e"></a>`unit`: `member`

### Declared structure

- <a id="s-037e5890aa"></a>`kind`: `"method"`
- <a id="s-8fbd7672cd"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [OperationContract](stove0-target-protocol-operationcontract.md)

## Governing policies

- <a id="pa-99baaceb6f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContract.validate_roles`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2c166a9e41531e3563015d63a4cfb5b82142c7f6492f9a43216696f9962ede05 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_roles",
  "owner": "stove0_target_protocol.OperationContract",
  "unit": "member"
}
```

</details>
