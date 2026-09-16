# stove0_target_protocol.TargetJobDeclaration.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobdeclarati-18e6f9f34f:6b40103067 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-658dd1dd22"></a>
- <a id="s-ffee0feaef"></a>`distribution`: `stove0-target-protocol`
- <a id="s-2e8ccf86ae"></a>`module`: `stove0_target_protocol`
- <a id="s-4ae3e756e1"></a>`name`: `canonical_claim_id`
- <a id="s-00b5cb691a"></a>`owner`: `stove0_target_protocol.TargetJobDeclaration`
- <a id="s-194d01e8fe"></a>`unit`: `member`

### Declared structure

- <a id="s-decb0328ed"></a>`kind`: `"classmethod"`
- <a id="s-6ebdb15181"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [TargetJobDeclaration](stove0-target-protocol-targetjobdeclaration.md)

## Governing policies

- <a id="pa-6d24cc2808"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobDeclaration.canonical_claim_id`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce37d886ad19a0ac3d68ecae839318ab693761b74debb6a9c61a07dd4ceba570 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_claim_id",
  "owner": "stove0_target_protocol.TargetJobDeclaration",
  "unit": "member"
}
```

</details>
