# stove0_protocol.WorkflowPreview.canonical_warnings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-canonical-warnings:fcc871d195 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ac64b0669"></a>
- <a id="s-459937f61d"></a>`distribution`: `stove0-protocol`
- <a id="s-fc40110e9c"></a>`module`: `stove0_protocol`
- <a id="s-5e9347e4d6"></a>`name`: `canonical_warnings`
- <a id="s-883c4f1d98"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-15d1b7ee6f"></a>`unit`: `member`

### Declared structure

- <a id="s-0ccf8c18c7"></a>`kind`: `"classmethod"`
- <a id="s-057cf3a7a4"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-80cb6c62c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.canonical_warnings`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7b5423e6722b57393af4b0c541de16fd0928e395d342dd3b53bfcfc7777e30b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_warnings",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```

</details>
