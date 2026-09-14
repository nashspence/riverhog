# stove0_core.ParentOutcomeBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-parentoutcomebinding:ba24ed3af7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58777b0240"></a>
- <a id="s-164442d04a"></a>`distribution`: `stove0-server`
- <a id="s-e77ca8c35a"></a>`module`: `stove0_core`
- <a id="s-d30da87dbe"></a>`name`: `ParentOutcomeBinding`
- <a id="s-a0cd4aa86e"></a>`unit`: `export`

### Declared structure

- <a id="s-61e240ca6b"></a>`kind`: `"class"`
- <a id="s-253a91d7f4"></a>`signature`: `"\"(claim: 'ClaimBinding', outcome_id: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-6b22789dcf"></a>`claim` | `'ClaimBinding'` | `required` |
| <a id="s-54307e3733"></a>`outcome_id` | `'str'` | `required` |

## Governing policies

- <a id="pa-353c6e29ca"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ParentOutcomeBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63a679d3e873b3f3af84bff6d5770c71d2daccdc0e8f53ed50e850d9baed313c -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "claim",
        "type": "'ClaimBinding'"
      },
      {
        "default": "required",
        "name": "outcome_id",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(claim: 'ClaimBinding', outcome_id: 'str') -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ParentOutcomeBinding",
  "unit": "export"
}
```
