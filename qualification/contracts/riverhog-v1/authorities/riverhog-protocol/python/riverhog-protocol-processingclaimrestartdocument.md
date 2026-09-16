# riverhog_protocol.ProcessingClaimRestartDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrestartdocument:689c762e66 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76c82fb3d3"></a>
- <a id="s-ef188b1b4d"></a>`distribution`: `riverhog-protocol`
- <a id="s-a02774a774"></a>`module`: `riverhog_protocol`
- <a id="s-e3d4ec07f0"></a>`name`: `ProcessingClaimRestartDocument`
- <a id="s-1db7fb9246"></a>`unit`: `export`

### Declared structure

- <a id="s-e6338ea845"></a>`kind`: `"class"`
- <a id="s-c5be82a6f1"></a>`signature`: `"'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"`

#### Validated model schema

<a id="s-445f8600e0"></a>
- <a id="s-a2aaf7a807"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f8e290dab"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-0833ed1442"></a>`lease_seconds` | no | type="integer"; minimum=30; maximum=86400 |  |

## Maintained corroboration

### Related interface records

- [__getitem__](riverhog-protocol-processingclaimrestartdocument-getitem.md)
- [get](riverhog-protocol-processingclaimrestartdocument-get.md)

## Governing policies

- <a id="pa-ff86f2192f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRestartDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e629369ff561444834c164b15ea7ca28e3b72c13b4ee9e814a5774f4de0bb96 -->

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
        },
        "lease_seconds": {
          "default": 1800,
          "maximum": 86400,
          "minimum": 30,
          "type": "integer"
        }
      },
      "required": [
        "fence"
      ],
      "type": "object"
    },
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], lease_seconds: Annotated[int, Ge(ge=30), Le(le=86400)] = 1800) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimRestartDocument",
  "unit": "export"
}
```
