# review0_target_contracts.REVIEW_MATERIALIZE_OPERATION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-ca1f95951d:fe05b716c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d1de53c09"></a>
- <a id="s-d0381489cc"></a>`distribution`: `review0-target-contracts`
- <a id="s-bc381edd38"></a>`module`: `review0_target_contracts`
- <a id="s-fb4e68ca00"></a>`name`: `REVIEW_MATERIALIZE_OPERATION`
- <a id="s-b4da0cb6b6"></a>`unit`: `export`

### Declared structure

- <a id="s-e94d14da9e"></a>`kind`: `"object"`
- <a id="s-c52deee5cb"></a>`type`: `"stove0_target_protocol.protocol.OperationContract"`

## Governing policies

- <a id="pa-cfbd8ee677"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_OPERATION`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 343b8b0cade03f37b8f47349fd5831f6298d92fc86e3a605be418418c343d993 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.protocol.OperationContract"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_OPERATION",
  "unit": "export"
}
```

</details>
