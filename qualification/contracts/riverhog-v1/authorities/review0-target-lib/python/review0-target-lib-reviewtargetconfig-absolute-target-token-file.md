# review0_target_lib.ReviewTargetConfig.absolute_target_token_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-reviewtargetconfig-abs-6d2fedcab6:a85d055dd9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c1c034fcf"></a>
- <a id="s-898e2811f4"></a>`distribution`: `review0-target-lib`
- <a id="s-5fdaf1b451"></a>`module`: `review0_target_lib`
- <a id="s-8412841ccf"></a>`name`: `absolute_target_token_file`
- <a id="s-6b9d639dd6"></a>`owner`: `review0_target_lib.ReviewTargetConfig`
- <a id="s-ead3cf2acb"></a>`unit`: `member`

### Declared structure

- <a id="s-8ba88bc31e"></a>`kind`: `"classmethod"`
- <a id="s-f9ae7d5847"></a>`signature`: `"\"(cls, value: 'Path') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [ReviewTargetConfig](review0-target-lib-reviewtargetconfig.md)

## Governing policies

- <a id="pa-4f8899607a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.ReviewTargetConfig.absolute_target_token_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79915bb611b0ba1987e60411f9c244a28f2c57240923bcb02cb4847721113e21 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Path') -> 'Path'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "absolute_target_token_file",
  "owner": "review0_target_lib.ReviewTargetConfig",
  "unit": "member"
}
```

</details>
