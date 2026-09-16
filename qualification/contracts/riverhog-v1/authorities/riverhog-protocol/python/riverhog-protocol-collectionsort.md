# riverhog_protocol.CollectionSort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionsort:46a98894b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39c50e19d8"></a>
- <a id="s-e58431c73e"></a>`distribution`: `riverhog-protocol`
- <a id="s-7669a3bf08"></a>`module`: `riverhog_protocol`
- <a id="s-67e8b740dc"></a>`name`: `CollectionSort`
- <a id="s-974fd785d5"></a>`unit`: `export`

### Declared structure

- <a id="s-1b60eb6b38"></a>`kind`: `"type-alias"`
- <a id="s-a139ac3e11"></a>`value`: `"typing.Literal['id', 'created_at', 'bytes', 'files']"`

## Governing policies

- <a id="pa-3d40b11abb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionSort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8326e13e65b4d632bc9462b9f4583954df35289e52ba56020910cbfac8ca24e0 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['id', 'created_at', 'bytes', 'files']"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionSort",
  "unit": "export"
}
```

</details>
