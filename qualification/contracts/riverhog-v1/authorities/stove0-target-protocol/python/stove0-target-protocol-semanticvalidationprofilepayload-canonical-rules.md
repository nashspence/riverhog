# stove0_target_protocol.SemanticValidationProfilePayload.canonical_rules

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidation-7bd789f0fa:16747dd3d5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56ea0b2f6b"></a>
- <a id="s-fb2bfc48c8"></a>`distribution`: `stove0-target-protocol`
- <a id="s-e0afa3662d"></a>`module`: `stove0_target_protocol`
- <a id="s-7f606d2c0a"></a>`name`: `canonical_rules`
- <a id="s-6aeb73f72c"></a>`owner`: `stove0_target_protocol.SemanticValidationProfilePayload`
- <a id="s-bc389796fd"></a>`unit`: `member`

### Declared structure

- <a id="s-8e1fc9369b"></a>`kind`: `"classmethod"`
- <a id="s-642ba20512"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfilePayload](stove0-target-protocol-semanticvalidationprofilepayload.md)

## Governing policies

- <a id="pa-d748802c7c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfilePayload.canonical_rules`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ef6781347afa7bc03d76d70da10698ca4105ae551ac451c87947dbd98b18fc8c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_rules",
  "owner": "stove0_target_protocol.SemanticValidationProfilePayload",
  "unit": "member"
}
```

</details>
