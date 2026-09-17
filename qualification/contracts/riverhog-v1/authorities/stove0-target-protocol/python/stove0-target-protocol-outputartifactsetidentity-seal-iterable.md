# stove0_target_protocol.OutputArtifactSetIdentity.seal_iterable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-outputartifactseti-c75a569941:189f96e9a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c16d86d3b8"></a>
- <a id="s-4786a14b7c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-8d66141654"></a>`module`: `stove0_target_protocol`
- <a id="s-0512013674"></a>`name`: `seal_iterable`
- <a id="s-4f37a43919"></a>`owner`: `stove0_target_protocol.OutputArtifactSetIdentity`
- <a id="s-0195ae3756"></a>`unit`: `member`

### Declared structure

- <a id="s-9609bc6e6b"></a>`kind`: `"classmethod"`
- <a id="s-dd3fbe5b7b"></a>`signature`: `"\"(cls, artifacts: 'Iterable[OutputArtifact]') -> 'OutputArtifactSetIdentity'\""`

## Maintained corroboration

### Related interface records

- [OutputArtifactSetIdentity](stove0-target-protocol-outputartifactsetidentity.md)

## Governing policies

- <a id="pa-1ee7165489"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.OutputArtifactSetIdentity.seal_iterable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 368a16095cde2bb0c417df707f44ef812e1e8606cc92573d57ec4cec9ac79231 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, artifacts: 'Iterable[OutputArtifact]') -> 'OutputArtifactSetIdentity'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal_iterable",
  "owner": "stove0_target_protocol.OutputArtifactSetIdentity",
  "unit": "member"
}
```

</details>
