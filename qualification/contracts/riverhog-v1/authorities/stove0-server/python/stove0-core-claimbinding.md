# stove0_core.ClaimBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-claimbinding:82a31f5db2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b809db080b"></a>
- <a id="s-94c1857fc6"></a>`distribution`: `stove0-server`
- <a id="s-6c8880c007"></a>`module`: `stove0_core`
- <a id="s-7936ede901"></a>`name`: `ClaimBinding`
- <a id="s-222ae778d6"></a>`unit`: `export`

### Declared structure

- <a id="s-3d22409ed8"></a>`kind`: `"class"`
- <a id="s-d9eb2f22ba"></a>`signature`: `"'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"`

#### Validated model schema

<a id="s-577bfcf6b0"></a>
- <a id="s-ec33b82180"></a>`title`: ClaimBinding
- <a id="s-5177d009da"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-897c4d7dda"></a>`claim_id` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-e99e03e44d"></a>`fence` | yes | type="integer"; minimum=1 |  |

## Governing policies

- <a id="pa-de4fba4083"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClaimBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35a78a2ecae184c142377f4ebf2fb20991e0280bd14e5b3355891c2b7e34683b -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "claim_id": {
          "maxLength": 160,
          "minLength": 1,
          "title": "Claim Id",
          "type": "string"
        },
        "fence": {
          "minimum": 1,
          "title": "Fence",
          "type": "integer"
        }
      },
      "required": [
        "claim_id",
        "fence"
      ],
      "title": "ClaimBinding",
      "type": "object"
    },
    "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ClaimBinding",
  "unit": "export"
}
```
