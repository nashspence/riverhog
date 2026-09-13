# riverhog_protocol.CatalogSyncCheckpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsynccheckpoint:aa221b8bb5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b897c77a22"></a>
| Field | Shape |
|---|---|
| <a id="s-508733814e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5a6f81591e"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e38dfd13d2"></a>`module` | "riverhog_protocol" |
| <a id="s-f303c7898e"></a>`name` | "CatalogSyncCheckpoint" |
| <a id="s-e958f819f3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-beddb74af8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncCheckpoint`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 848ee4660dc11d5f2567059d9ec3b7e3495c0933ffc6df512b5d4b2da0eae7ec -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "23f1a9cd803ee925c19334eab9c8b6024bbbee97eb6be8c601b45924f2bf8d9e",
    "signature": "\"(*, format: Literal['riverhog-catalog-sync/v1'] = 'riverhog-catalog-sync/v1', source_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], authorization_view_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=64, max_length=64, pattern='^[0-9a-f]{64}$', ascii_only=None)], catalog_cursor: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=4096, pattern=None, ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncCheckpoint",
  "unit": "export"
}
```
