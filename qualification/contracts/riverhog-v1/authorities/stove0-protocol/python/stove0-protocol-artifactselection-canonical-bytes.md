# stove0_protocol.ArtifactSelection.canonical_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-canonical-bytes:9f6323221f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c520787627"></a>
- <a id="s-298690f22e"></a>`distribution`: `stove0-protocol`
- <a id="s-02173b3442"></a>`module`: `stove0_protocol`
- <a id="s-fc74ce74ef"></a>`name`: `canonical_bytes`
- <a id="s-d49f898c20"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-a141891c56"></a>`unit`: `member`

### Declared structure

- <a id="s-d9156a757b"></a>`kind`: `"method"`
- <a id="s-98887710a1"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-0bc55e24e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.canonical_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae690c46b866d8cb8159997355a7b6c715bfd0754966a5f90606e53039bb369a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_bytes",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```

</details>
