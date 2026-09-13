# stove0_media_archive_target_contracts.XMP_SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-xmp-4873d649bc:e8f3023c01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50a3087767"></a>
| Field | Shape |
|---|---|
| <a id="s-2854e5e885"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-98ff1e2350"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-10d2f3ba38"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-acc438a9ac"></a>`name` | "XMP_SOURCE_ROLE" |
| <a id="s-88aeab546e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d97e6c09e9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.XMP_SOURCE_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8642da4ac237dbb48698db8b8e53b9fbcea0a23fcfefb9f5242dbbd27b9927c3 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.xmp-source/v1"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "XMP_SOURCE_ROLE",
  "unit": "export"
}
```
