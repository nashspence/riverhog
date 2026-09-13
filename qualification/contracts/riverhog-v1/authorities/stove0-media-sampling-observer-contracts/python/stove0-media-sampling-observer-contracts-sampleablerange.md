# stove0_media_sampling_observer_contracts.SampleableRange

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-60d1a1baab:479d698010 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c87acab97"></a>
| Field | Shape |
|---|---|
| <a id="s-18aa440fb4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-ca6c5bba05"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-cab3e5577d"></a>`module` | "stove0_media_sampling_observer_contracts" |
| <a id="s-8b9011fb4d"></a>`name` | "SampleableRange" |
| <a id="s-08637c79bd"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c0bd866587"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.SampleableRange`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7862da27f5a9e7c2945f2b6280249e5f2d1e04d7e8cf65268cc1bc75106fb68 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0f6c17a4144ba9bfacd01e668546fd8d744823cf27a929ea5e9a11839f6be09d",
    "signature": "'(*, start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "SampleableRange",
  "unit": "export"
}
```
