# review0_target_lib.SamplerConfig.absolute_token_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-samplerconfig-absolute-token-file:163670951f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bb972e4fa"></a>
- <a id="s-4a87720906"></a>`distribution`: `review0-target-lib`
- <a id="s-6bff6788e7"></a>`module`: `review0_target_lib`
- <a id="s-fe536a4671"></a>`name`: `absolute_token_file`
- <a id="s-be4b33fb85"></a>`owner`: `review0_target_lib.SamplerConfig`
- <a id="s-411d413e77"></a>`unit`: `member`

### Declared structure

- <a id="s-e6f52efdab"></a>`kind`: `"classmethod"`
- <a id="s-7e53472096"></a>`signature`: `"\"(cls, value: 'Path') -> 'Path'\""`

## Maintained corroboration

### Related interface records

- [SamplerConfig](review0-target-lib-samplerconfig.md)

## Governing policies

- <a id="pa-3eaa807281"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.SamplerConfig.absolute_token_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29d88b01578d9a65bf4b923bcaf70975ca0dc7f213f5b004842b5bd3c73688b5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Path') -> 'Path'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "absolute_token_file",
  "owner": "review0_target_lib.SamplerConfig",
  "unit": "member"
}
```

</details>
