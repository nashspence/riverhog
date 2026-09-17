# stove0_core.TargetPort.cancel_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-cancel-job:736c8dadb3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f16903ecf2"></a>
- <a id="s-1e27bbe89d"></a>`distribution`: `stove0-server`
- <a id="s-c588eda300"></a>`module`: `stove0_core`
- <a id="s-7343b9ddc1"></a>`name`: `cancel_job`
- <a id="s-b054ee93d4"></a>`owner`: `stove0_core.TargetPort`
- <a id="s-ce178d24ee"></a>`unit`: `member`

### Declared structure

- <a id="s-fe1f10b3c1"></a>`kind`: `"method"`
- <a id="s-c4b2bdc962"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetJobRequest \| AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-2a5ee3da46"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.cancel_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 313321c259a07117a8e929b45dc2a4e24adf9891707aa8691e249aa90a1683c9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel_job",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```

</details>
