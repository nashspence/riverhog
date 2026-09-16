# stove0_protocol.WorkIdentity.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity-canonical-inputs:e304d89b15 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c87b5ac1ea"></a>
- <a id="s-f27a95a3f9"></a>`distribution`: `stove0-protocol`
- <a id="s-149f590e96"></a>`module`: `stove0_protocol`
- <a id="s-c9da2f4880"></a>`name`: `canonical_inputs`
- <a id="s-02155fda85"></a>`owner`: `stove0_protocol.WorkIdentity`
- <a id="s-2bb5b54120"></a>`unit`: `member`

### Declared structure

- <a id="s-69f8396804"></a>`kind`: `"classmethod"`
- <a id="s-e7de5d3aca"></a>`signature`: `"\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkIdentity](stove0-protocol-workidentity.md)

## Governing policies

- <a id="pa-8c88be7c58"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity.canonical_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bfd6f000e4f5f28a6318a2ee16da62313f42fd13e2969bb0abc660ed4efa073 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CollectionRootRef, ...]') -> 'tuple[CollectionRootRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_inputs",
  "owner": "stove0_protocol.WorkIdentity",
  "unit": "member"
}
```
