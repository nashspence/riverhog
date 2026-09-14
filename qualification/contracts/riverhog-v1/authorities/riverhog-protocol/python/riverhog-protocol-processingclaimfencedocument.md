# riverhog_protocol.ProcessingClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimfencedocument:aebabca317 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e9c3fb5e0"></a>
- <a id="s-d774b48fcc"></a>`distribution`: `riverhog-protocol`
- <a id="s-20f0f1e79b"></a>`module`: `riverhog_protocol`
- <a id="s-145e9046b9"></a>`name`: `ProcessingClaimFenceDocument`
- <a id="s-153b43a728"></a>`unit`: `export`

### Declared structure

- <a id="s-6e4f5a087f"></a>`kind`: `"class"`
- <a id="s-fd249713d4"></a>`signature`: `"'(*, fence: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-79182f253c"></a>
- <a id="s-5b766fe5e1"></a>`title`: ProcessingClaimFenceDocument
- <a id="s-e838297b11"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9eaab391aa"></a>`fence` | yes | type="integer"; minimum=1 |  |

## Governing policies

- <a id="pa-530b1c7343"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimFenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46d3bebe10e851c824d84704f59f565e5e5fb1d9e3ce3f43e038a2c54367a12e -->

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
        }
      },
      "required": [
        "fence"
      ],
      "title": "ProcessingClaimFenceDocument",
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimFenceDocument",
  "unit": "export"
}
```
