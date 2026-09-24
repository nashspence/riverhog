# stove0_protocol.ArtifactSelection.roots

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-roots:f57314954b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48db7397d2"></a>
- <a id="s-4d6400bb26"></a>`distribution`: `stove0-protocol`
- <a id="s-34ad09c61a"></a>`module`: `stove0_protocol`
- <a id="s-03333e0438"></a>`name`: `roots`
- <a id="s-ce6540067c"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-7e2c65456f"></a>`unit`: `member`

### Declared structure

- <a id="s-4078b39add"></a>`kind`: `"method"`
- <a id="s-3c674eb0ab"></a>`signature`: `"\"(self) -> 'tuple[CollectionRootIdentityRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-159860805e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.roots`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 119e3865a85d71f201d899da4fb33ea6fe0f9c86e6137b4d44b53f602d0b1a49 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[CollectionRootIdentityRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "roots",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```

</details>
