# stove0_protocol.BRANCH_SETTLEMENT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branch-settlement-format:0cfadd57d1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b4cef0d4af"></a>
- <a id="s-7e46c08287"></a>`distribution`: `stove0-protocol`
- <a id="s-c6c21dc393"></a>`module`: `stove0_protocol`
- <a id="s-78b17fe5a8"></a>`name`: `BRANCH_SETTLEMENT_FORMAT`
- <a id="s-b4ce21cae8"></a>`unit`: `export`

### Declared structure

- <a id="s-9e6a19bb09"></a>`kind`: `"constant"`
- <a id="s-54d57d205c"></a>`value`: `"stove0-branch-settlement/v1"`

## Governing policies

- <a id="pa-0f95cb00a4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BRANCH_SETTLEMENT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1aa98b7c6b5472aaacd81d15a7b6cc83b662c7067e0d7921d73566c8f47e3677 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-branch-settlement/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BRANCH_SETTLEMENT_FORMAT",
  "unit": "export"
}
```

</details>
