# riverhog_storage_adapter_protocol.ObjectReadRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectreadrequest:e055e24268 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23b3da4fbd"></a>
| Field | Shape |
|---|---|
| <a id="s-76b8e4bbc7"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4a3e76f2a5"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-056463f18c"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-e5ccef96ef"></a>`name` | "ObjectReadRequest" |
| <a id="s-97a6598fda"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.ObjectReadRequest.validate_range](riverhog-storage-adapter-protocol-objectreadrequest-validate-range.md)

## Governing policies

- <a id="pa-ebd7c4eec6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 90df1d7e2732c7943e991851f0aa5f69500e9dbc06d3312bdbe91b9189a795be -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8f080cf57778767bde220ff2111c9ba3501ad753c8492b1759db04b8451d108d",
    "signature": "'(*, object: riverhog_storage_adapter_protocol.protocol.ObjectLocator, expected_bytes: Annotated[int, Ge(ge=0)], offset: Annotated[int | None, Ge(ge=0)] = None, size: Annotated[int | None, Ge(ge=0)] = None) -> None'"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ObjectReadRequest",
  "unit": "export"
}
```
