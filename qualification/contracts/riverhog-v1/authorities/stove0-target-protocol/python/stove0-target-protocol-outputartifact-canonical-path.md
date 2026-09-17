# stove0_target_protocol.OutputArtifact.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifact-canonical-path:6985372b58 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-438951f669"></a>
- <a id="s-ce6c629cac"></a>`distribution`: `stove0-target-protocol`
- <a id="s-20d53336f5"></a>`module`: `stove0_target_protocol`
- <a id="s-da6776556f"></a>`name`: `canonical_path`
- <a id="s-e5658fe3c4"></a>`owner`: `stove0_target_protocol.OutputArtifact`
- <a id="s-d2619808b5"></a>`unit`: `member`

### Declared structure

- <a id="s-3a0561d953"></a>`kind`: `"classmethod"`
- <a id="s-fae3e77547"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [OutputArtifact](stove0-target-protocol-outputartifact.md)

## Governing policies

- <a id="pa-1db68d0e8e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifact.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e2a19e5a55fd01a28ace01c20f2e47df062fb53f990a5baf6e30cda1fc40bd24 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_path",
  "owner": "stove0_target_protocol.OutputArtifact",
  "unit": "member"
}
```

</details>
