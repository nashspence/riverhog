# riverhog_client.CollectionUploadIdempotencyKey

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-collectionuploadidempotencykey:8aee9af537 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b46bb64f4"></a>
- <a id="s-de32b47334"></a>`distribution`: `riverhog-client`
- <a id="s-1b7b5b51ba"></a>`module`: `riverhog_client`
- <a id="s-fe482ad98f"></a>`name`: `CollectionUploadIdempotencyKey`
- <a id="s-05523e3403"></a>`unit`: `export`

### Declared structure

- <a id="s-03169e5ccf"></a>`kind`: `"type-alias"`
- <a id="s-9dbbba6096"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"`

## Governing policies

- <a id="pa-ad5e85b292"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CollectionUploadIdempotencyKey`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d215266839349b74c44f220b377e4be34cf3e0985ff93c70b0852d9b1d90f72a -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CollectionUploadIdempotencyKey",
  "unit": "export"
}
```

</details>
