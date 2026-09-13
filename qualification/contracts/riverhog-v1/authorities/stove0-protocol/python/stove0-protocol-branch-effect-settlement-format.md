# stove0_protocol.BRANCH_EFFECT_SETTLEMENT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branch-effect-settlement-format:c9f1956067 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0963aeba9"></a>
| Field | Shape |
|---|---|
| <a id="s-9c74d0c469"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-7a601da911"></a>`distribution` | "stove0-protocol" |
| <a id="s-c2cf00e222"></a>`module` | "stove0_protocol" |
| <a id="s-001b6c76eb"></a>`name` | "BRANCH_EFFECT_SETTLEMENT_FORMAT" |
| <a id="s-8fa49d4dfc"></a>`unit` | "export" |

## Governing policies

- <a id="pa-01e34dc7b3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BRANCH_EFFECT_SETTLEMENT_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac42e538762af584fbc9c1f97a3f1b2b21b7fd83a9c594810cbfb27e16b7f9b5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-branch-effect-settlement/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BRANCH_EFFECT_SETTLEMENT_FORMAT",
  "unit": "export"
}
```
