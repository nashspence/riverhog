# stove0_protocol.WorkflowPreview.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-verify-digest:04054e0a16 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42e6994cb0"></a>
- <a id="s-1107e890c5"></a>`distribution`: `stove0-protocol`
- <a id="s-1fa694f3cf"></a>`module`: `stove0_protocol`
- <a id="s-4b34356180"></a>`name`: `verify_digest`
- <a id="s-b8e2c01b10"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-aeeda048d6"></a>`unit`: `member`

### Declared structure

- <a id="s-4160704ed8"></a>`kind`: `"method"`
- <a id="s-a3f5b70903"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-e5f575fbc9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f0de3d3d9e979377d85de243f6abc9a9292f3e2c51d9d3431016cc1dafc1fc96 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```

</details>
