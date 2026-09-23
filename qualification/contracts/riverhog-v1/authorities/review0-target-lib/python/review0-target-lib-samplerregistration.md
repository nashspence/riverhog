# review0_target_lib.SamplerRegistration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-samplerregistration:3d44c778c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-11f079d6dc"></a>
- <a id="s-a8e68394ab"></a>`distribution`: `review0-target-lib`
- <a id="s-5d9adb3fab"></a>`module`: `review0_target_lib`
- <a id="s-b243cef77e"></a>`name`: `SamplerRegistration`
- <a id="s-5ec41752fa"></a>`unit`: `export`

### Declared structure

- <a id="s-e01ec6e1ff"></a>`kind`: `"class"`
- <a id="s-a5dcd36400"></a>`signature`: `"\"(id: 'str', client: 'ReviewSamplerClient', descriptor_sha256: 'str', image_digest: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-71f18a90db"></a>`id` | `'str'` | `required` |
| <a id="s-9f7be32903"></a>`client` | `'ReviewSamplerClient'` | `required` |
| <a id="s-cb3061bbab"></a>`descriptor_sha256` | `'str'` | `required` |
| <a id="s-3a5cf17b28"></a>`image_digest` | `'str'` | `required` |

## Maintained corroboration

### Related interface records

- [descriptor](review0-target-lib-samplerregistration-descriptor.md)

## Governing policies

- <a id="pa-9326de9268"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.SamplerRegistration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c504f65344acd12e2fd1134f4cc31e00b233aa40bdd9f96498bb3b98e9b76829 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "client",
        "type": "'ReviewSamplerClient'"
      },
      {
        "default": "required",
        "name": "descriptor_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "image_digest",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(id: 'str', client: 'ReviewSamplerClient', descriptor_sha256: 'str', image_digest: 'str') -> None\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "SamplerRegistration",
  "unit": "export"
}
```

</details>
