# stove0_protocol.branch_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branch-result-kind:e8b7d8688a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63fb240859"></a>
- <a id="s-63d5dfa899"></a>`distribution`: `stove0-protocol`
- <a id="s-d3526ae9a1"></a>`module`: `stove0_protocol`
- <a id="s-468ea23dd1"></a>`name`: `branch_result_kind`
- <a id="s-d1bef27194"></a>`unit`: `export`

### Declared structure

- <a id="s-00525a51f0"></a>`kind`: `"function"`
- <a id="s-f9f9351aec"></a>`signature`: `"'(branch: \\'BranchDeclaration\\', branch_sets: \\'Mapping[str, BranchSetPlan]\\') -> \"Literal[\\'collection\\', \\'external-effect\\', \\'coordination\\']\"'"`

## Governing policies

- <a id="pa-031501c00b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.branch_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7501d21fd08fe461846e4c181fb9a2a11116dadd7e04f8a4d731c2b79c2a9a31 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "'(branch: \\'BranchDeclaration\\', branch_sets: \\'Mapping[str, BranchSetPlan]\\') -> \"Literal[\\'collection\\', \\'external-effect\\', \\'coordination\\']\"'"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "branch_result_kind",
  "unit": "export"
}
```

</details>
