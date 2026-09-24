# stove0_operator_contracts.OperatorWorkflowPreviewRequest.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-operatorworkflo-117e5c97ab:ba44605f93 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5b5571c2d"></a>
- <a id="s-6f4f20f356"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-420ea35cf5"></a>`module`: `stove0_operator_contracts`
- <a id="s-aae1b0c4ac"></a>`name`: `canonical_inputs`
- <a id="s-a54dbe722c"></a>`owner`: `stove0_operator_contracts.OperatorWorkflowPreviewRequest`
- <a id="s-130c6f583d"></a>`unit`: `member`

### Declared structure

- <a id="s-24931a7a7c"></a>`kind`: `"classmethod"`
- <a id="s-98f3c1390d"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [OperatorWorkflowPreviewRequest](stove0-operator-contracts-operatorworkflowpreviewrequest.md)

## Governing policies

- <a id="pa-b803fe7d65"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.OperatorWorkflowPreviewRequest.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3958496b1248ed9e70809ed26d8b2b8b6b853b1d856201bb2852582fd5057a46 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootIdentityRef, ...]') -> 'tuple[CollectionRootIdentityRef, ...]'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "canonical_inputs",
  "owner": "stove0_operator_contracts.OperatorWorkflowPreviewRequest",
  "unit": "member"
}
```

</details>
