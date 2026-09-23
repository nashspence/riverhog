# review0_target_contracts.REVIEW_AUDIO_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-audio-role:3e9dfda0e2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fd26b00165"></a>
- <a id="s-55c636e39d"></a>`distribution`: `review0-target-contracts`
- <a id="s-9baf4f9cbc"></a>`module`: `review0_target_contracts`
- <a id="s-9021d15860"></a>`name`: `REVIEW_AUDIO_ROLE`
- <a id="s-8e14a2c2ff"></a>`unit`: `export`

### Declared structure

- <a id="s-8acae8bc6d"></a>`kind`: `"constant"`
- <a id="s-71a61aca0c"></a>`value`: `"stove0.review.audio/v1"`

## Governing policies

- <a id="pa-e1cdc6bea8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_AUDIO_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 669fd68ac96f63612c75f172ce1d5ad7edf11264b33a07b478b4fe11c2a13c31 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.review.audio/v1"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_AUDIO_ROLE",
  "unit": "export"
}
```

</details>
