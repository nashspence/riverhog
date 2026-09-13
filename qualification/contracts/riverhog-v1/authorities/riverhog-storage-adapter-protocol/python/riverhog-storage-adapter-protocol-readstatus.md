# riverhog_storage_adapter_protocol.ReadStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readstatus:57c1fe427a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-119d3c48aa"></a>
| Field | Shape |
|---|---|
| <a id="s-5c4a04c649"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-049185a14a"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-de74da1e28"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-f331165b3f"></a>`name` | "ReadStatus" |
| <a id="s-f701940259"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ReadStatus.canonical_objects](riverhog-storage-adapter-protocol-readstatus-canonical-objects.md)

## Governing policies

- <a id="pa-00785524c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1406d98478b6c12ff7d8e9c863a408c9539ab53c4fa3879f27befd9ab8a59cdb -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c2e15c25cf8ffc82c41b134eb06692ae3c3138226cfdc381c64e277774f403a6",
    "signature": "'(*, objects: Annotated[tuple[riverhog_storage_adapter_protocol.protocol.ObjectLocator, ...], MinLen(min_length=1)], readiness: riverhog_storage_adapter_protocol.protocol.ReadRequested | riverhog_storage_adapter_protocol.protocol.ReadReady | riverhog_storage_adapter_protocol.protocol.ReadExpired) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadStatus",
  "unit": "export"
}
```
