# stove0_target_protocol.TargetCallbackAcknowledgement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcallbackacknowledgement:7e07d79cd2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63e29c9597"></a>
- <a id="s-b3df149366"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1494aa5b74"></a>`module`: `stove0_target_protocol`
- <a id="s-eacab8333a"></a>`name`: `TargetCallbackAcknowledgement`
- <a id="s-42e2d9e38b"></a>`unit`: `export`

### Declared structure

- <a id="s-ce185c3dc9"></a>`kind`: `"class"`
- <a id="s-5b90fa8011"></a>`signature`: `"'(*, accepted: Literal[True] = True) -> None'"`

#### Validated model schema

<a id="s-efa050e563"></a>
- <a id="s-869849d3f0"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9052757ae9"></a>`accepted` | no | type="boolean"; const=true |  |

## Governing policies

- <a id="pa-8bedfac872"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetCallbackAcknowledgement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d733225942e278290bd807dd6400782af70c97dfc4cda7993e6d78c1715e574c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "const": true,
          "default": true,
          "type": "boolean"
        }
      },
      "type": "object"
    },
    "signature": "'(*, accepted: Literal[True] = True) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetCallbackAcknowledgement",
  "unit": "export"
}
```
