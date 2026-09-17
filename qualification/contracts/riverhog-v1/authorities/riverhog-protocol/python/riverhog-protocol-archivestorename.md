# riverhog_protocol.ArchiveStoreName

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-archivestorename:97f223c92f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6701497571"></a>
- <a id="s-47b72269d9"></a>`distribution`: `riverhog-protocol`
- <a id="s-8ef9242574"></a>`module`: `riverhog_protocol`
- <a id="s-797b90bd90"></a>`name`: `ArchiveStoreName`
- <a id="s-d106559502"></a>`unit`: `export`

### Declared structure

- <a id="s-1b730ee415"></a>`kind`: `"type-alias"`
- <a id="s-1244e420d0"></a>`value`: `"typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_archive_store_name>)]"`

## Governing policies

- <a id="pa-c85d3972dc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArchiveStoreName`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3dcbce675a1cdc5c0473988df807785575e2bc5cb254dcff6dd4503a3da2f4b -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, FieldInfo(annotation=NoneType, required=True, metadata=[_PydanticGeneralMetadata(pattern='^[a-z0-9]+(?:-[a-z0-9]+)*$')]), AfterValidator(func=<function validate_archive_store_name>)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ArchiveStoreName",
  "unit": "export"
}
```

</details>
