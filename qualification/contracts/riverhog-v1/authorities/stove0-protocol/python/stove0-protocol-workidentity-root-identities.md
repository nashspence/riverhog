# stove0_protocol.WorkIdentity.root_identities

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity-root-identities:7c54b76c20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3f538d3870"></a>
- <a id="s-f35fb65243"></a>`distribution`: `stove0-protocol`
- <a id="s-b5b90f6e5e"></a>`module`: `stove0_protocol`
- <a id="s-b87c32b2ea"></a>`name`: `root_identities`
- <a id="s-5bc2a984cf"></a>`owner`: `stove0_protocol.WorkIdentity`
- <a id="s-a5dfe2ce4d"></a>`unit`: `member`

### Declared structure

- <a id="s-3175021d62"></a>`kind`: `"method"`
- <a id="s-172ebaa6fa"></a>`signature`: `"\"(self) -> 'tuple[CollectionRootIdentity, ...]'\""`

## Maintained corroboration

### Related interface records

- [WorkIdentity](stove0-protocol-workidentity.md)

## Governing policies

- <a id="pa-5469c35382"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity.root_identities`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d01ffc86047100083478a49a3e44e3046505bb41f9aca27a211ee2e2b986be9b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[CollectionRootIdentity, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "root_identities",
  "owner": "stove0_protocol.WorkIdentity",
  "unit": "member"
}
```

</details>
