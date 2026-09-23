# stove0_core.TargetPort.get_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-get-job:3875401765 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d992a1c10d"></a>
- <a id="s-eee4bbec7a"></a>`distribution`: `stove0-server`
- <a id="s-d5e836b7cb"></a>`module`: `stove0_core`
- <a id="s-0dfaabc540"></a>`name`: `get_job`
- <a id="s-7dfa7e1da0"></a>`owner`: `stove0_core.TargetPort`
- <a id="s-441d74b1d9"></a>`unit`: `member`

### Declared structure

- <a id="s-9c9a545bfc"></a>`kind`: `"method"`
- <a id="s-14f72369de"></a>`signature`: `"\"(self, registration_id: 'str', request: 'TargetJobRequest \| AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""`

## Maintained corroboration

### Related interface records

- [TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-440922dadd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.get_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 882fcaf253d8eb1ea4c4bac5b71b2e48dc28814c180279aba670a94648d87fca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest | AcceptedTargetJob', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_job",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```

</details>
