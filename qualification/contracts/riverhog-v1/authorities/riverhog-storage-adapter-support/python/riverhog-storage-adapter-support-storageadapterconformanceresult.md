# riverhog_storage_adapter_support.StorageAdapterConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-c664c77473:3d01d04996 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18facb70da"></a>
| Field | Shape |
|---|---|
| <a id="s-cb6b0404bf"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ffedee9ffc"></a>`distribution` | "riverhog-storage-adapter-support" |
| <a id="s-384c5d2673"></a>`module` | "riverhog_storage_adapter_support" |
| <a id="s-d0a87d23b2"></a>`name` | "StorageAdapterConformanceResult" |
| <a id="s-d65496649d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_support.StorageAdapterConformanceResult.validate_exact_coverage](riverhog-storage-adapter-support-storageadapterconformanceresult-validate-exact-coverage.md)

## Governing policies

- <a id="pa-840d814743"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1079ccce0ad320671cb35a5a45a4960435d03bcd359f4076e3d5b987d4ad1054 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "af805afd4d0dffb329ba08dde971222ad008eab2aec7afc4abc0303dd1956caf",
    "signature": "\"(*, format: Literal['riverhog-storage-adapter-conformance-result/v1'] = 'riverhog-storage-adapter-conformance-result/v1', protocol: Literal['riverhog-storage-adapter/v1'] = 'riverhog-storage-adapter/v1', status: Literal['conformant'] = 'conformant', coverage: Literal['complete'] = 'complete', descriptor: riverhog_storage_adapter_protocol.protocol.AdapterDescriptor, checks: tuple[str, ...]) -> None\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterConformanceResult",
  "unit": "export"
}
```
