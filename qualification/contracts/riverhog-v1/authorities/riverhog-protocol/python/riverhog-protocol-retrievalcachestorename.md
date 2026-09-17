# riverhog_protocol.RetrievalCacheStoreName

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalcachestorename:6d5012b9a2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-041304bc94"></a>
- <a id="s-00c68a8c88"></a>`distribution`: `riverhog-protocol`
- <a id="s-21cd563bf3"></a>`module`: `riverhog_protocol`
- <a id="s-ebf5fd6820"></a>`name`: `RetrievalCacheStoreName`
- <a id="s-b58f750178"></a>`unit`: `export`

### Declared structure

- <a id="s-15a304c88b"></a>`kind`: `"type-alias"`
- <a id="s-40b29a9a92"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_archive_store_name>)]"`

## Governing policies

- <a id="pa-d912feb789"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalCacheStoreName`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9039f96973d72e84d16a555dc2b498943cb11b37c2550e28f0ebd639b50a1e4 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_archive_store_name>)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RetrievalCacheStoreName",
  "unit": "export"
}
```

</details>
