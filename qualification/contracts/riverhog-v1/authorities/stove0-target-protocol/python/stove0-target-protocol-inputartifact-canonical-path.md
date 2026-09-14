# stove0_target_protocol.InputArtifact.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-inputartifact-canonical-path:67b9eff029 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c35db14c8a"></a>
- <a id="s-b8a33f3010"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f67ef903cb"></a>`module`: `stove0_target_protocol`
- <a id="s-a9663b7bc8"></a>`name`: `canonical_path`
- <a id="s-0eb645be90"></a>`owner`: `stove0_target_protocol.InputArtifact`
- <a id="s-696507a314"></a>`unit`: `member`

### Declared structure

- <a id="s-5dad685371"></a>`kind`: `"classmethod"`
- <a id="s-8160b7d508"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [InputArtifact](stove0-target-protocol-inputartifact.md)

## Governing policies

- <a id="pa-075611eba7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.InputArtifact.canonical_path`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 324b2d4cffb8d49670063603123e3e36c433c389b30bb17ee47d6f6b37b1daf8 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_path",
  "owner": "stove0_target_protocol.InputArtifact",
  "unit": "member"
}
```
