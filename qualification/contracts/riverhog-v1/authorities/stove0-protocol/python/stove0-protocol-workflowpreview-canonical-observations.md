# stove0_protocol.WorkflowPreview.canonical_observations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-canonical-9198536183:8bed942bd5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42acfcc53c"></a>
- <a id="s-5ef6606c2d"></a>`distribution`: `stove0-protocol`
- <a id="s-06b4e6a6cf"></a>`module`: `stove0_protocol`
- <a id="s-2b85bc448a"></a>`name`: `canonical_observations`
- <a id="s-933d477a2e"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-63d2ef72f4"></a>`unit`: `member`

### Declared structure

- <a id="s-0ec893d4f2"></a>`kind`: `"classmethod"`
- <a id="s-74f40248df"></a>`signature`: `"\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-b1e7ccebbb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.canonical_observations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf1b98cad773b74a77d004c6e685401df1a9c4fff749686d484fe90f93b1fe9e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_observations",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```

</details>
