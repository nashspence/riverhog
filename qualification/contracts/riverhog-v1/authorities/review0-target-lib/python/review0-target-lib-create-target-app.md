# review0_target_lib.create_target_app

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-create-target-app:cfe80dc562 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3db4260455"></a>
- <a id="s-e1b24670b5"></a>`distribution`: `review0-target-lib`
- <a id="s-bc7e331625"></a>`module`: `review0_target_lib`
- <a id="s-1453fc6fb2"></a>`name`: `create_target_app`
- <a id="s-f429cae867"></a>`unit`: `export`

### Declared structure

- <a id="s-e8bcc2c475"></a>`kind`: `"function"`
- <a id="s-fd5301fdb8"></a>`signature`: `"\"(*, service: 'str', title: 'str', token: 'str', target: 'ReviewTarget') -> 'FastAPI'\""`

## Governing policies

- <a id="pa-21a35b5940"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.create_target_app`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebec438734aa77361491a42c6a35f1f2629b5461b58ce0b4885bdf8618d9e830 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, service: 'str', title: 'str', token: 'str', target: 'ReviewTarget') -> 'FastAPI'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "create_target_app",
  "unit": "export"
}
```

</details>
