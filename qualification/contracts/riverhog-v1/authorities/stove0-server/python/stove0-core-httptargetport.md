# stove0_core.HttpTargetPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport:c8d062e1c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18c39ad9d3"></a>
- <a id="s-259dd11ae2"></a>`distribution`: `stove0-server`
- <a id="s-33431e7cdc"></a>`module`: `stove0_core`
- <a id="s-a434595b55"></a>`name`: `HttpTargetPort`
- <a id="s-a0ea424092"></a>`unit`: `export`

### Declared structure

- <a id="s-41af8e3ed6"></a>`kind`: `"class"`
- <a id="s-a94ce75930"></a>`signature`: `"\"(registrations: 'dict[str, TargetClient]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel_job](stove0-core-httptargetport-cancel-job.md)
- [contract](stove0-core-httptargetport-contract.md)
- [get_job](stove0-core-httptargetport-get-job.md)
- [preflight](stove0-core-httptargetport-preflight.md)
- [put_job](stove0-core-httptargetport-put-job.md)

## Governing policies

- <a id="pa-b14131879a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d96e47b9e956dbea5733b442280cea22e218953aecd9bfabd7fbbfb8dbf7df0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registrations: 'dict[str, TargetClient]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "HttpTargetPort",
  "unit": "export"
}
```

</details>
