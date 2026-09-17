# riverhog_archive_contracts.AGE_UPLOAD_STATE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-age-upload-state-format:b571c8fcfd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-02203c3bf0"></a>
- <a id="s-7ead89f671"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-5a988d387a"></a>`module`: `riverhog_archive_contracts`
- <a id="s-a49cae194b"></a>`name`: `AGE_UPLOAD_STATE_FORMAT`
- <a id="s-50ed239326"></a>`unit`: `export`

### Declared structure

- <a id="s-43f4b4429a"></a>`kind`: `"constant"`
- <a id="s-6e3cd6953e"></a>`value`: `"age-v1-scrypt-resumable"`

## Governing policies

- <a id="pa-76794c5966"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.AGE_UPLOAD_STATE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ec18e732f85305132e2d02cb156f0a0495c4f990cf438d6b6fa739438ca5cfae -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "age-v1-scrypt-resumable"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "AGE_UPLOAD_STATE_FORMAT",
  "unit": "export"
}
```

</details>
