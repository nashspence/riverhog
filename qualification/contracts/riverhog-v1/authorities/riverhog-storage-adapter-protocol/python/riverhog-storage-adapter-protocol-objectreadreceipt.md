# riverhog_storage_adapter_protocol.ObjectReadReceipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectreadreceipt:f0fbaa1083 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc85bc1f0e"></a>
| Field | Shape |
|---|---|
| <a id="s-ff13228a94"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4e5ba58d59"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-f3b568daec"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-3843e6cda2"></a>`name` | "ObjectReadReceipt" |
| <a id="s-a930d0f1cf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectReadReceipt.validate_range](riverhog-storage-adapter-protocol-objectreadreceipt-validate-range.md)

## Governing policies

- <a id="pa-cc7323be97"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadReceipt`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa6990ebc10d22b74298a4db79b8566785ba65ada99fa371c236dcb5193ec327 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c00ab742b44d1b708bac1d252dd1dd6fb4882bdec441eb330f63746d635820ce",
    "signature": "'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, total_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int, Ge(ge=0)], read_bytes: Annotated[int, Ge(ge=0)]) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectReadReceipt",
  "unit": "export"
}
```
