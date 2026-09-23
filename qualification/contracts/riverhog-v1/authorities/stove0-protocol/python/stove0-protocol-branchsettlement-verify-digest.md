# stove0_protocol.BranchSettlement.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsettlement-verify-digest:c5ecddf92c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c2018296a"></a>
- <a id="s-8b0c12820c"></a>`distribution`: `stove0-protocol`
- <a id="s-e9f1fe43e0"></a>`module`: `stove0_protocol`
- <a id="s-85b153813b"></a>`name`: `verify_digest`
- <a id="s-de82ed28da"></a>`owner`: `stove0_protocol.BranchSettlement`
- <a id="s-f576f07d93"></a>`unit`: `member`

### Declared structure

- <a id="s-d3b05aa20c"></a>`kind`: `"method"`
- <a id="s-5b576ac57d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [BranchSettlement](stove0-protocol-branchsettlement.md)

## Governing policies

- <a id="pa-c99cd4dc88"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSettlement.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: be9062332959d9211f595e92eba6b5f688f9e5a4760e0489abc096958cc51008 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_digest",
  "owner": "stove0_protocol.BranchSettlement",
  "unit": "member"
}
```

</details>
