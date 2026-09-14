# stove0_media_archive_target_contracts.validate_av1_opus_archive_intent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-val-07d7797dc8:f728071341 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-335f14e2e3"></a>
- <a id="s-fcc0cfe28e"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-09e1edd832"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-bd87724b57"></a>`name`: `validate_av1_opus_archive_intent`
- <a id="s-5f4ee30ca5"></a>`unit`: `export`

### Declared structure

- <a id="s-8e2c4bfa8a"></a>`kind`: `"function"`
- <a id="s-cafb9d5d7c"></a>`signature`: `"\"(intent: 'Mapping[str, object]') -> 'None'\""`

## Governing policies

- <a id="pa-d06ea147e3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.validate_av1_opus_archive_intent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07967f2ea240bac9ea23f1b7759c8cb5e3d1bb3293db2a0269288a9ee6e56e74 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(intent: 'Mapping[str, object]') -> 'None'\""
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "validate_av1_opus_archive_intent",
  "unit": "export"
}
```
