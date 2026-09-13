# riverhog_storage_adapter_protocol.SmallObjectWriteRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-smallob-bb7878c9d1:27eaf5a542 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-058b84cc00"></a>
| Field | Shape |
|---|---|
| <a id="s-d447a313bd"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-64a43edcb7"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-b21ea1961e"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-704366a094"></a>`name` | "SmallObjectWriteRequest" |
| <a id="s-ac1f34235c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.SmallObjectWriteRequest.validate_replacement_fence](riverhog-storage-adapter-protocol-smallobjectwriterequest-validate-replacement-fence.md)
- [riverhog_storage_adapter_protocol.SmallObjectWriteRequest.canonical_metadata](riverhog-storage-adapter-protocol-smallobjectwriterequest-canonical-metadata.md)
- [riverhog_storage_adapter_protocol.SmallObjectWriteRequest.canonical_path](riverhog-storage-adapter-protocol-smallobjectwriterequest-canonical-path.md)

## Governing policies

- <a id="pa-c39f4266c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.SmallObjectWriteRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d72d00fc538a4d76f37fa0a43c0c292ea127adab7184bec5460c0e7341ae348 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "80a724477b2bedfe5fa69bfd96c571cd25a684c741a0e8d5a715c15bfc25c270",
    "signature": "\"(*, object_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], content_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], required_identity_assertions: Annotated[dict[str, str], MaxLen(max_length=64)], placement: Literal['archive', 'immediate'], mode: Literal['create_only', 'replace_current'], expected_current_stored_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, stored_bytes: Annotated[int, Ge(ge=0)], stored_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "SmallObjectWriteRequest",
  "unit": "export"
}
```
