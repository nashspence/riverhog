# stove0_target_protocol.OperationContract.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-operationcontract-8f630a2358:d3bbf7959d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a6172a4a0e"></a>
- <a id="s-abd719d05c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-2cf0a66da2"></a>`module`: `stove0_target_protocol`
- <a id="s-a50922a021"></a>`name`: `verify_digest`
- <a id="s-58ff36ea63"></a>`owner`: `stove0_target_protocol.OperationContract`
- <a id="s-6d81207eca"></a>`unit`: `member`

### Declared structure

- <a id="s-e9c0bfe135"></a>`kind`: `"method"`
- <a id="s-bd39c2e6d6"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [OperationContract](stove0-target-protocol-operationcontract.md)

## Governing policies

- <a id="pa-c537982003"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.OperationContract.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 380301b20cf161c988b02c5b615da9f16d2eed186f65ca06139676ea9406e2ec -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.OperationContract",
  "unit": "member"
}
```

</details>
