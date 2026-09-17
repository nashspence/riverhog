# stove0_target_protocol.TargetInputAuthority.from_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetinputauthori-256e046243:536e378dca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-103dcafaa2"></a>
- <a id="s-35570ecc46"></a>`distribution`: `stove0-target-protocol`
- <a id="s-21ac78fac0"></a>`module`: `stove0_target_protocol`
- <a id="s-d87debf1c3"></a>`name`: `from_selection`
- <a id="s-f26a4b3443"></a>`owner`: `stove0_target_protocol.TargetInputAuthority`
- <a id="s-5be79ce6a8"></a>`unit`: `member`

### Declared structure

- <a id="s-f6dd436d29"></a>`kind`: `"classmethod"`
- <a id="s-497a441cf4"></a>`signature`: `"\"(cls, selection: 'ArtifactSelection') -> 'TargetInputAuthority'\""`

## Maintained corroboration

### Related interface records

- [TargetInputAuthority](stove0-target-protocol-targetinputauthority.md)

## Governing policies

- <a id="pa-2774ce1af0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetInputAuthority.from_selection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae385ce732ab9fac5c67c502c1bb42f3ffd348f8aae960db0f42d9d75a29ccfb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, selection: 'ArtifactSelection') -> 'TargetInputAuthority'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "from_selection",
  "owner": "stove0_target_protocol.TargetInputAuthority",
  "unit": "member"
}
```

</details>
