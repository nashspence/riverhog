# review0_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-rclone-de-29cf6a3d0d:d8c1958f5a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b5535d000"></a>
- <a id="s-48c6f46323"></a>`distribution`: `review0-target-contracts`
- <a id="s-123d9f9a9a"></a>`module`: `review0_target_contracts`
- <a id="s-17e7e351fa"></a>`name`: `REVIEW_RCLONE_DELIVER_OPERATION`
- <a id="s-f9a76cbaf8"></a>`unit`: `export`

### Declared structure

- <a id="s-27043758b5"></a>`kind`: `"object"`
- <a id="s-aa9deff8eb"></a>`type`: `"stove0_target_protocol.protocol.OperationContract"`

## Governing policies

- <a id="pa-6905b693e5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_RCLONE_DELIVER_OPERATION`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b270108ada7b951c1e7feb138c33c9147b92fc4ffb9b2c86604ba13d70ac253 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.protocol.OperationContract"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_RCLONE_DELIVER_OPERATION",
  "unit": "export"
}
```

</details>
