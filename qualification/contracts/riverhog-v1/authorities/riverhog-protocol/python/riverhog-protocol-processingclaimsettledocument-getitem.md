# riverhog_protocol.ProcessingClaimSettleDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsettledo-c2a8407ea6:d3bbee4c50 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-517c13314a"></a>
- <a id="s-c92da69e9f"></a>`distribution`: `riverhog-protocol`
- <a id="s-d90f7713ec"></a>`module`: `riverhog_protocol`
- <a id="s-b65d2265c5"></a>`name`: `__getitem__`
- <a id="s-834b7a67ea"></a>`owner`: `riverhog_protocol.ProcessingClaimSettleDocument`
- <a id="s-0e6ec918aa"></a>`unit`: `member`

### Declared structure

- <a id="s-58c4c334e6"></a>`kind`: `"method"`
- <a id="s-1f619aafdf"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimSettleDocument](riverhog-protocol-processingclaimsettledocument.md)

## Governing policies

- <a id="pa-07eeff7ed8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSettleDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7a610aaebec8ee65c2e62c563bc4d5811dc3507c8bad4766101b8a390a062757 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.ProcessingClaimSettleDocument",
  "unit": "member"
}
```

</details>
