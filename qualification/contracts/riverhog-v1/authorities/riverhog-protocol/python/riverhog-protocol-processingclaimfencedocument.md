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

<!-- exact-contract-value: b602a87fd9041146a8dbc6a4cd27ae3a284f144a7a825974150f7a4a67a48041 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "fence": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "fence"
      ],
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
