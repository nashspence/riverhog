# stove0_target_protocol.OutputArtifactSetIdentity.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactseti-6e3202465e:6b0605f489 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e92ded363"></a>
- <a id="s-15dcc94dee"></a>`distribution`: `stove0-target-protocol`
- <a id="s-cfd9781ec3"></a>`module`: `stove0_target_protocol`
- <a id="s-e827d4376e"></a>`name`: `seal`
- <a id="s-7cca3dfa45"></a>`owner`: `stove0_target_protocol.OutputArtifactSetIdentity`
- <a id="s-898d31e6b7"></a>`unit`: `member`

### Declared structure

- <a id="s-80d3f3f746"></a>`kind`: `"classmethod"`
- <a id="s-242018fb9f"></a>`signature`: `"\"(cls, artifacts: 'tuple[OutputArtifact, ...]') -> 'OutputArtifactSetIdentity'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.OutputArtifactSetIdentity](stove0-target-protocol-outputartifactsetidentity.md)

## Governing policies

- <a id="pa-512e322d25"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactSetIdentity.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39fcd5090f13e82758259de4245a9549cde042c5932a68e9015453c22e18ae86 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, artifacts: 'tuple[OutputArtifact, ...]') -> 'OutputArtifactSetIdentity'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.OutputArtifactSetIdentity",
  "unit": "member"
}
```
