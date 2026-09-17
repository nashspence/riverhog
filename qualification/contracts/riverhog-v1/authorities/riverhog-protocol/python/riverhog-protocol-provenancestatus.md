# riverhog_protocol.ProvenanceStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-provenancestatus:4952e91c8f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d868fdb8ec"></a>
- <a id="s-add9ea0509"></a>`distribution`: `riverhog-protocol`
- <a id="s-c2f6e39c6e"></a>`module`: `riverhog_protocol`
- <a id="s-fc2ba1a1b0"></a>`name`: `ProvenanceStatus`
- <a id="s-d0dc0ac880"></a>`unit`: `export`

### Declared structure

- <a id="s-79e720c3ba"></a>`kind`: `"type-alias"`
- <a id="s-87d309b446"></a>`value`: `"typing.Literal['captured', 'omitted']"`

## Governing policies

- <a id="pa-fffbbb6ed7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProvenanceStatus`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 050caaa20bd587f29b179b1b6b72dad316b60f3a3938948332d47f9f3fca2df5 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['captured', 'omitted']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProvenanceStatus",
  "unit": "export"
}
```

</details>
