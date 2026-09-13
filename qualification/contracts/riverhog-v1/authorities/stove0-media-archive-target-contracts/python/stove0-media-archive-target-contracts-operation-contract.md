# stove0_media_archive_target_contracts.operation_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-ope-1f38f16511:457bc79012 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1f03974796"></a>
| Field | Shape |
|---|---|
| <a id="s-fbe79641a4"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-854b2cd078"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-36bd417b89"></a>`module` | "stove0_media_archive_target_contracts" |
| <a id="s-27876a92ba"></a>`name` | "operation_contract" |
| <a id="s-07bcc51721"></a>`unit` | "export" |

## Governing policies

- <a id="pa-cd545e037c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.operation_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c038b3a4e32f79835b8e6ae50a1cd823250b384fd0f3e077881a2a5b1f3e089 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(operation_id: 'str') -> 'OperationContract'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "operation_contract",
  "unit": "export"
}
```
