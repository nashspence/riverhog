# stove0_protocol.WorkflowPreviewRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequest-verify-digest:45858c6f1f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f46ca52eca"></a>
- <a id="s-51783b781c"></a>`distribution`: `stove0-protocol`
- <a id="s-84d3aa2bde"></a>`module`: `stove0_protocol`
- <a id="s-fd38d5be6c"></a>`name`: `verify_digest`
- <a id="s-7d8146eba4"></a>`owner`: `stove0_protocol.WorkflowPreviewRequest`
- <a id="s-3c4c5e4db1"></a>`unit`: `member`

### Declared structure

- <a id="s-23ad6116d6"></a>`kind`: `"method"`
- <a id="s-082ce4f942"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewRequest](stove0-protocol-workflowpreviewrequest.md)

## Governing policies

- <a id="pa-72eed4bf7e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4919f46b6f26dcbd6e8d17a8ba7ae8cdb9043c3028e13c2b3f90a9c14cbc9544 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.WorkflowPreviewRequest",
  "unit": "member"
}
```

</details>
