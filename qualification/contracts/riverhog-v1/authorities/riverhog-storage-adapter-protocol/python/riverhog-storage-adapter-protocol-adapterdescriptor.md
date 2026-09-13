# riverhog_storage_adapter_protocol.AdapterDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-adapterdescriptor:1c87f504a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65e1a436bb"></a>
| Field | Shape |
|---|---|
| <a id="s-6e65e45851"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-b5b2c11f15"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-728a5464c2"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-ee265a69aa"></a>`name` | "AdapterDescriptor" |
| <a id="s-9419e0d1de"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.AdapterDescriptor.validate_segment_limits](riverhog-storage-adapter-protocol-adapterdescriptor-validate-segment-limits.md)

## Governing policies

- <a id="pa-77afa7ee79"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.AdapterDescriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ffc3a22c99a28aa31298739fc121b5c3b7534c306c68081d707ff7364ad2f58 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "9cee17550dd897297e60b7829813a6b432932a360b38b800c81292b1cbdda754",
    "signature": "\"(*, protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], read_mode: Literal['immediate', 'restore_required'], minimum_nonfinal_segment_bytes: Annotated[int, Ge(ge=1)], maximum_segment_bytes: Annotated[int | None, Ge(ge=1)] = None, maximum_segment_count: Annotated[int | None, Ge(ge=1)] = None) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "AdapterDescriptor",
  "unit": "export"
}
```
