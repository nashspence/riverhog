# review0_target_lib.sampler_registrations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-sampler-registrations:5801431eeb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f18aa03b56"></a>
- <a id="s-6005338623"></a>`distribution`: `review0-target-lib`
- <a id="s-70b123114b"></a>`module`: `review0_target_lib`
- <a id="s-f8306b7cf2"></a>`name`: `sampler_registrations`
- <a id="s-fd9dca288f"></a>`unit`: `export`

### Declared structure

- <a id="s-d29ef02a54"></a>`kind`: `"function"`
- <a id="s-a54556ff10"></a>`signature`: `"\"(config: 'ReviewTargetConfig') -> 'tuple[SamplerRegistration, ...]'\""`

## Governing policies

- <a id="pa-fcd3919e12"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.sampler_registrations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 861faa5b30276308be79eb6f53c2e7ce65508a8d312aa021cdee68309cb3e2bf -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config: 'ReviewTargetConfig') -> 'tuple[SamplerRegistration, ...]'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "sampler_registrations",
  "unit": "export"
}
```

</details>
