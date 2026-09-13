# stove0_ffprobe_sampling_observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer:288d49a5a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9a18c46c53"></a>
| Field | Shape |
|---|---|
| <a id="s-e92274064c"></a>`candidate_id` | "python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer" |
| <a id="s-0833c9b2e9"></a>`distribution` | "stove0-ffprobe-sampling-observer" |
| <a id="s-440cfab869"></a>`exports` | additional keys=`FfprobeSamplingObserver` |
| <a id="s-75ba0c4450"></a>`module` | "stove0_ffprobe_sampling_observer" |

## Governing policies

- <a id="pa-975e9d9696"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../../../evidence/sources.md#src-7974c3bc25) — `reference/stove0/observers/ffprobe-sampling/src/stove0_ffprobe_sampling_observer/__init__.py`

### Machine authority

- `/external_contract/python/34`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13a550f2ffd4ee5aa79334e8de77e4bc62835b8e91b7d34926f677f9bd1abf4f -->

```json
{
  "candidate_id": "python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer",
  "distribution": "stove0-ffprobe-sampling-observer",
  "exports": {
    "FfprobeSamplingObserver": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'ObserverDescriptor'\""
        },
        "execution_evidence": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, str]'\""
        },
        "observe": {
          "kind": "method",
          "signature": "\"(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'\""
        }
      },
      "signature": "\"(*, ffprobe: 'str' = 'ffprobe', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
    }
  },
  "module": "stove0_ffprobe_sampling_observer"
}
```
