# stove0_exiftool_observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-exiftool-observer:stove0-exiftool-observer:2ef88cdf84 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76ccaac00c"></a>
| Field | Shape |
|---|---|
| <a id="s-b22f2180b2"></a>`candidate_id` | "python:stove0-exiftool-observer:stove0_exiftool_observer" |
| <a id="s-876516bad2"></a>`distribution` | "stove0-exiftool-observer" |
| <a id="s-559e92d645"></a>`exports` | additional keys=`ExiftoolObserver` |
| <a id="s-addb9ca852"></a>`module` | "stove0_exiftool_observer" |

## Governing policies

- <a id="pa-e6da46deb6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-exiftool-observer:stove0_exiftool_observer](../../../evidence/sources.md#src-18b5d27762) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/__init__.py`

### Machine authority

- `/external_contract/python/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af9cd29f090f66202f0f67a8d4245f7674a2458d5f7f5eeaa89f477282ec1e7b -->

```json
{
  "candidate_id": "python:stove0-exiftool-observer:stove0_exiftool_observer",
  "distribution": "stove0-exiftool-observer",
  "exports": {
    "ExiftoolObserver": {
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
      "signature": "\"(*, exiftool: 'str' = 'exiftool', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
    }
  },
  "module": "stove0_exiftool_observer"
}
```
