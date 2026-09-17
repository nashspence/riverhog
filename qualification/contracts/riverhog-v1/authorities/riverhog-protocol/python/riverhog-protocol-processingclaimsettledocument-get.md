# riverhog_protocol.ProcessingClaimSettleDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsettledocument-get:f7f301da85 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-142372c775"></a>
- <a id="s-6aa2d04ce7"></a>`distribution`: `riverhog-protocol`
- <a id="s-b5f819ac2f"></a>`module`: `riverhog_protocol`
- <a id="s-a030338fc2"></a>`name`: `get`
- <a id="s-4c7b959b7c"></a>`owner`: `riverhog_protocol.ProcessingClaimSettleDocument`
- <a id="s-3abd8218bf"></a>`unit`: `member`

### Declared structure

- <a id="s-b40fc521c7"></a>`kind`: `"method"`
- <a id="s-3b26f6961d"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimSettleDocument](riverhog-protocol-processingclaimsettledocument.md)

## Governing policies

- <a id="pa-0df4f17ae2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSettleDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a1924c2a6d456b0f0fd69d0d6d2d58304e2a1d434bace13b8b884a2dcc8e709 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimSettleDocument",
  "unit": "member"
}
```

</details>
