# stove0_media_archive_target_contracts.MediaFieldPreference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-med-266e049f42:73f810e28b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e1ceeb836"></a>
| Field | Shape |
|---|---|
| <a id="s-019f7b1b14"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4b72f4ae74"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-e1e383d29a"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-b0e76192b9"></a>`name` | "MediaFieldPreference" |
| <a id="s-6095b93a50"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_contracts.MediaFieldPreference.unique_fields](stove0-media-archive-target-contracts-mediafieldpreference-unique-fields.md)

## Governing policies

- <a id="pa-5c562f5f23"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.MediaFieldPreference`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 72a9396871d5b8588ae648f74983e3f1440e1428e545f5500e0b73b1b42f2a29 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "a116c8bee1c7caaea6c6f3063ea638d2000da8abd190574442ce7410d1e56e4d",
    "signature": "\"(*, name: Literal['capture-time', 'creator', 'device-make', 'device-model', 'gps-latitude', 'gps-longitude'], fields: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "MediaFieldPreference",
  "unit": "export"
}
```
