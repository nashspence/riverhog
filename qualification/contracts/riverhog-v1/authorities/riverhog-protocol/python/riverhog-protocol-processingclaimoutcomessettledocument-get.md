# riverhog_protocol.ProcessingClaimOutcomesSettleDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimoutcomes-c6f98197b6:0278cbb3b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-64d2c301a9"></a>
- <a id="s-487e32c50e"></a>`distribution`: `riverhog-protocol`
- <a id="s-83fdc548e5"></a>`module`: `riverhog_protocol`
- <a id="s-e70619561b"></a>`name`: `get`
- <a id="s-7fd7604ea6"></a>`owner`: `riverhog_protocol.ProcessingClaimOutcomesSettleDocument`
- <a id="s-a2b1e2e590"></a>`unit`: `member`

### Declared structure

- <a id="s-4a7f63f0f3"></a>`kind`: `"method"`
- <a id="s-0b962a557b"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimOutcomesSettleDocument](riverhog-protocol-processingclaimoutcomessettledocument.md)

## Governing policies

- <a id="pa-7af610af69"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimOutcomesSettleDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 564f9451ce44b71fd73aa733c4ba8ed09bac20d89084ff3032a17f01bc495c81 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimOutcomesSettleDocument",
  "unit": "member"
}
```

</details>
