# stove0_media_archive_target_contracts.SOURCE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-source-role:0cc6c1d13a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e4fee2eeb"></a>
- <a id="s-253aae3701"></a>`distribution`: `stove0-media-archive-target-contracts`
- <a id="s-63c17ffa26"></a>`module`: `stove0_media_archive_target_contracts`
- <a id="s-f2d8f9e697"></a>`name`: `SOURCE_ROLE`
- <a id="s-64f6ef1cc9"></a>`unit`: `export`

### Declared structure

- <a id="s-55c14374e4"></a>`kind`: `"constant"`
- <a id="s-4eb32496dd"></a>`value`: `"stove0.media.source/v1"`

## Governing policies

- <a id="pa-3e41947496"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../../../evidence/sources.md#src-dfeb5229f2) — `reference/stove0/targets/media-archive/contracts/src/stove0_media_archive_target_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_contracts.SOURCE_ROLE`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13d208621bfab3843ea137948c5dc832dca3348a44a4e8682de88f013e4d205a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.source/v1"
  },
  "distribution": "stove0-media-archive-target-contracts",
  "module": "stove0_media_archive_target_contracts",
  "name": "SOURCE_ROLE",
  "unit": "export"
}
```
