# stove0_protocol.WorkflowPreviewPayload.canonical_warnings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewpayload-ca-09dbcb55e0:e44da1949f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1168f33382"></a>
- <a id="s-605881e59b"></a>`distribution`: `stove0-protocol`
- <a id="s-9d26cd6f7f"></a>`module`: `stove0_protocol`
- <a id="s-bb398c7b13"></a>`name`: `canonical_warnings`
- <a id="s-fc8f60dda4"></a>`owner`: `stove0_protocol.WorkflowPreviewPayload`
- <a id="s-59fc76ae33"></a>`unit`: `member`

### Declared structure

- <a id="s-4ea28265d2"></a>`kind`: `"classmethod"`
- <a id="s-8aee995c9a"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreviewPayload](stove0-protocol-workflowpreviewpayload.md)

## Governing policies

- <a id="pa-1a5994809a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewPayload.canonical_warnings`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b2b329aff58a0b98dce7f031e6e9e0b707ebb1f567d623b388549d9dcbce845 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_warnings",
  "owner": "stove0_protocol.WorkflowPreviewPayload",
  "unit": "member"
}
```

</details>
