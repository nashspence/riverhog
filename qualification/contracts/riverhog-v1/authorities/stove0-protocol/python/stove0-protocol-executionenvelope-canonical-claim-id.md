# stove0_protocol.ExecutionEnvelope.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope-canonical-claim-id:2182bac16c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-225ebd75c2"></a>
- <a id="s-27509f7415"></a>`distribution`: `stove0-protocol`
- <a id="s-e4b5b4959d"></a>`module`: `stove0_protocol`
- <a id="s-40dd57f58c"></a>`name`: `canonical_claim_id`
- <a id="s-5d192690e5"></a>`owner`: `stove0_protocol.ExecutionEnvelope`
- <a id="s-74560e2b67"></a>`unit`: `member`

### Declared structure

- <a id="s-37aaa37eac"></a>`kind`: `"classmethod"`
- <a id="s-af3dd49167"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [ExecutionEnvelope](stove0-protocol-executionenvelope.md)

## Governing policies

- <a id="pa-2fddf9830a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope.canonical_claim_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 383ffc4ea0552bb6ea505613c3f44962314618e5595ad5ec8f5e8a28615bd2cd -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_claim_id",
  "owner": "stove0_protocol.ExecutionEnvelope",
  "unit": "member"
}
```

</details>
