# riverhog_age.ResumableAgeScryptSession

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-resumableagescryptsession:cb1bb8fc4e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7532348f68"></a>
- <a id="s-ea5b78c6c5"></a>`distribution`: `riverhog-age`
- <a id="s-3ba7af96dd"></a>`module`: `riverhog_age`
- <a id="s-21c4d8bea8"></a>`name`: `ResumableAgeScryptSession`
- <a id="s-1ecfd8c0b9"></a>`unit`: `export`

### Declared structure

- <a id="s-b1ca61a0a8"></a>`kind`: `"class"`
- <a id="s-72f0aeb874"></a>`signature`: `"\"(*, header: 'bytes', payload_nonce: 'bytes', file_key: 'bytes')\""`

## Maintained corroboration

### Related interface records

- [age_aligned_unit_plans](riverhog-age-resumableagescryptsession-age-aligned-unit-plans.md)
- [age_prefix](riverhog-age-resumableagescryptsession-age-prefix.md)
- [create](riverhog-age-resumableagescryptsession-create.md)
- [decrypt_chunk](riverhog-age-resumableagescryptsession-decrypt-chunk.md)
- [encrypt_plaintext](riverhog-age-resumableagescryptsession-encrypt-plaintext.md)
- [encrypt_chunk](riverhog-age-resumableagescryptsession-encrypt-chunk.md)
- [encrypt_part](riverhog-age-resumableagescryptsession-encrypt-part.md)
- [export_state](riverhog-age-resumableagescryptsession-export-state.md)
- [from_state](riverhog-age-resumableagescryptsession-from-state.md)

## Governing policies

- <a id="pa-84ee315f33"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.ResumableAgeScryptSession`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9637ff24f037e7b547797b475143c394c5bbc1a0a97894d83adf0cd92c6e192c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, header: 'bytes', payload_nonce: 'bytes', file_key: 'bytes')\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "ResumableAgeScryptSession",
  "unit": "export"
}
```

</details>
