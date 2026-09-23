# stove0_protocol.branch_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branch-work:fae629bedc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6c7fa9e19d"></a>
- <a id="s-5cdc2d52b2"></a>`distribution`: `stove0-protocol`
- <a id="s-9c373984d4"></a>`module`: `stove0_protocol`
- <a id="s-0a1effb31a"></a>`name`: `branch_work`
- <a id="s-4c31c593a2"></a>`unit`: `export`

### Declared structure

- <a id="s-972ee09964"></a>`kind`: `"function"`
- <a id="s-888aef6cd5"></a>`signature`: `"\"(branch: 'BranchDeclaration') -> 'WorkIdentity'\""`

## Governing policies

- <a id="pa-493ff33a30"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.branch_work`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c134c96e9722929d74ad99f147fed348f77f4c015468e8f6103b84176f9db0c3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(branch: 'BranchDeclaration') -> 'WorkIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "branch_work",
  "unit": "export"
}
```

</details>
