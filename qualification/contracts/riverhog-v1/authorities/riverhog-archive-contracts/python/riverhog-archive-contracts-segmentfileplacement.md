# riverhog_archive_contracts.SegmentFilePlacement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-segmentfileplacement:e8e3a513f6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6657af26d"></a>
| Field | Shape |
|---|---|
| <a id="s-aaadaeeb0e"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-0f3165e2dc"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-45b152ebd2"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-3bbaef7019"></a>`name` | "SegmentFilePlacement" |
| <a id="s-7e5f1a5a88"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.SegmentFilePlacement.to_mapping](riverhog-archive-contracts-segmentfileplacement-to-mapping.md)
- [riverhog_archive_contracts.SegmentFilePlacement.from_mapping](riverhog-archive-contracts-segmentfileplacement-from-mapping.md)

## Governing policies

- <a id="pa-138142de7c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SegmentFilePlacement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 564aa4f38305789d2cb276c60de6dc412cfc2f3ed2ad0a2f32351d56b2e69c2a -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "offset",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "file_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(path: 'str', offset: 'int', bytes: 'int', file_bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "SegmentFilePlacement",
  "unit": "export"
}
```
