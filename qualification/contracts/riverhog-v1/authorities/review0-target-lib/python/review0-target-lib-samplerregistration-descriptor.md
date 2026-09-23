# review0_target_lib.SamplerRegistration.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-samplerregistration-descriptor:d8302e09e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5affdd49f1"></a>
- <a id="s-6b22249373"></a>`distribution`: `review0-target-lib`
- <a id="s-2fa49b7ae4"></a>`module`: `review0_target_lib`
- <a id="s-9d986afaf5"></a>`name`: `descriptor`
- <a id="s-8c0afeb633"></a>`owner`: `review0_target_lib.SamplerRegistration`
- <a id="s-bd59de0969"></a>`unit`: `member`

### Declared structure

- <a id="s-4aad36fb2b"></a>`kind`: `"method"`
- <a id="s-3b50f04abd"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [SamplerRegistration](review0-target-lib-samplerregistration.md)

## Governing policies

- <a id="pa-ba29833e1b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.SamplerRegistration.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 687926d6739afde23ad1016140d4274ac73085ad815e62bf322735b506dd5d1e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "descriptor",
  "owner": "review0_target_lib.SamplerRegistration",
  "unit": "member"
}
```

</details>
