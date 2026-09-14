# riverhog_protocol.ProcessingClaimRenewDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrenewdocument:55743c6830 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-67d8290270"></a>
- <a id="s-409aa45f28"></a>`distribution`: `riverhog-protocol`
- <a id="s-ae7fd162ea"></a>`module`: `riverhog_protocol`
- <a id="s-a6c8ba6b13"></a>`name`: `ProcessingClaimRenewDocument`
- <a id="s-e58af8205b"></a>`unit`: `export`

### Declared structure

- <a id="s-8620053107"></a>`kind`: `"class"`
- <a id="s-c69303d0ee"></a>`signature`: `"'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"`

#### Validated model schema

<a id="s-0e65013f90"></a>
- <a id="s-0e82ed8776"></a>`title`: ProcessingClaimRenewDocument
- <a id="s-0095a52f9f"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6ad8973f0"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-c75d18c697"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400 |  |

## Governing policies

- <a id="pa-842e0b955a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRenewDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 177bc1190f39d0af46ba59e71e8bee6b2706bd9b6a863b893b2ebf0a899c9f33 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        },
        "lease_seconds": {
          "default": 1800,
          "maximum": 86400,
          "minimum": 30,
          "title": "Lease Seconds",
          "type": "integer"
        }
      },
      "required": [
        "fence"
      ],
      "title": "ProcessingClaimRenewDocument",
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimRenewDocument",
  "unit": "export"
}
```
