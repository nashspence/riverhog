# riverhog_protocol.OmittedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-omittedfileprovenancebinding:ec3d5472bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76a121b9ba"></a>
- <a id="s-2c93b17ba5"></a>`distribution`: `riverhog-protocol`
- <a id="s-5dd170f00b"></a>`module`: `riverhog_protocol`
- <a id="s-45c2cabfb1"></a>`name`: `OmittedFileProvenanceBinding`
- <a id="s-d2a0760302"></a>`unit`: `export`

### Declared structure

- <a id="s-d734252520"></a>`kind`: `"class"`
- <a id="s-a47f922e5a"></a>`signature`: `"\"(*, status: Literal['omitted'], omission_reason: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-16d95d7ac2"></a>
- <a id="s-6b04c07349"></a>`title`: OmittedFileProvenanceBinding
- <a id="s-d7df20c576"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-efb6bed35d"></a>`omission_reason` | yes | type="string"; minLength=1; pattern="^\\S(?:[\\s\\S]*\\S)?$" |  |
| <a id="s-17183c1914"></a>`status` | yes | type="string"; const="omitted" |  |

## Governing policies

- <a id="pa-9f8ee361ad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.OmittedFileProvenanceBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 450319e61dfb7e3a99fc20400f2694428a92669a1d9179bc86687dfe4bf48415 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "omission_reason": {
          "minLength": 1,
          "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
          "title": "Omission Reason",
          "type": "string"
        },
        "status": {
          "const": "omitted",
          "title": "Status",
          "type": "string"
        }
      },
      "required": [
        "status",
        "omission_reason"
      ],
      "title": "OmittedFileProvenanceBinding",
      "type": "object"
    },
    "signature": "\"(*, status: Literal['omitted'], omission_reason: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "OmittedFileProvenanceBinding",
  "unit": "export"
}
```
