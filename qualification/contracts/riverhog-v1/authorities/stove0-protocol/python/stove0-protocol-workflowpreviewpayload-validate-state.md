# stove0_protocol.WorkflowPreviewPayload.validate_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-va-d6966cab70:08c727f1c2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e0a85e825"></a>
- <a id="s-403a673150"></a>`distribution`: `stove0-protocol`
- <a id="s-d4abde9715"></a>`module`: `stove0_protocol`
- <a id="s-8f710917f7"></a>`name`: `validate_state`
- <a id="s-1dc14d9864"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-6b18a8b327"></a>`unit`: `member`

### Declared structure

- <a id="s-ecac6bc9c8"></a>`kind`: `"method"`
- <a id="s-51edde2fe5"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-d00ba88845"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.validate_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6dde4ee005bd97cbad771a1f59d052163fe86091f6dbf4d2352829d19d8b2185 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_state",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
