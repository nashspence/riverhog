# stove0_media_sampling_observer_contracts.MediaSamplingArtifactFacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-sampling-observer-contracts:stove0-media-sampling-observer-contracts-79fcd99c7d:ec2fd28ce6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-sampling-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-95c516e5d7"></a>
| Field | Shape |
|---|---|
| <a id="s-0725ce1b53"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-047d3862a0"></a>`distribution` | "stove0-media-sampling-observer-contracts" |
| <a id="s-8a2ed58ca1"></a>`module` | "stove0_media_sampling_observer_contracts" |
| <a id="s-5a0fa7462f"></a>`name` | "MediaSamplingArtifactFacts" |
| <a id="s-3fe2f01d71"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_sampling_observer_contracts.MediaSamplingArtifactFacts.validate_ranges](stove0-media-sampling-observer-contracts-mediasamplingartifactfacts-validate-ranges.md)

## Governing policies

- <a id="pa-487bf5145c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../../../evidence/sources.md#src-b9344d06d7) — `reference/stove0/observers/contracts/media-sampling/src/stove0_media_sampling_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_sampling_observer_contracts.MediaSamplingArtifactFacts`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1cafe506187510601462dbe4b94f5c5b7e3792f0b558ae1a4913f05efdf77b79 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "94887d96e49df2efae09e9520e4bda3993b49202807d4d242ac22c277735f2c8",
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], duration_ms: Annotated[int, Ge(ge=1)], sampleable_ranges: Annotated[tuple[stove0_media_sampling_observer_contracts.contracts.SampleableRange, ...], MinLen(min_length=1)]) -> None'"
  },
  "distribution": "stove0-media-sampling-observer-contracts",
  "module": "stove0_media_sampling_observer_contracts",
  "name": "MediaSamplingArtifactFacts",
  "unit": "export"
}
```
