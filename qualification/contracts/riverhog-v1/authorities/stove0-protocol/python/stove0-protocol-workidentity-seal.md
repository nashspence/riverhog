# stove0_protocol.WorkIdentity.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workidentity-seal:8c38e96c29 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54efdb9c44"></a>
- <a id="s-3359e4d07a"></a>`distribution`: `stove0-protocol`
- <a id="s-0506fcf507"></a>`module`: `stove0_protocol`
- <a id="s-5cbf597eca"></a>`name`: `seal`
- <a id="s-577bf042b2"></a>`owner`: `stove0_protocol.WorkIdentity`
- <a id="s-5b6bf60a13"></a>`unit`: `member`

### Declared structure

- <a id="s-11ad83df0a"></a>`kind`: `"classmethod"`
- <a id="s-e93f210749"></a>`signature`: `"\"(cls, payload: 'WorkPayload') -> 'WorkIdentity'\""`

## Maintained corroboration

### Related interface records

- [WorkIdentity](stove0-protocol-workidentity.md)

## Governing policies

- <a id="pa-23ac2a93ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.WorkIdentity.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9bb209ff0bbfa16f7f7f40dfe4e457fa646ea6871901331090c6063e61eb1dea -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'WorkPayload') -> 'WorkIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.WorkIdentity",
  "unit": "member"
}
```

</details>
