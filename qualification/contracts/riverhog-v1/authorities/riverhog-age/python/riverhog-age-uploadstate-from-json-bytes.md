# riverhog_age.UploadState.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-uploadstate-from-json-bytes:0de8b4933a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-483be8879a"></a>
- <a id="s-292a93855a"></a>`distribution`: `riverhog-age`
- <a id="s-81b2e9fb4b"></a>`module`: `riverhog_age`
- <a id="s-8218df492f"></a>`name`: `from_json_bytes`
- <a id="s-da29899864"></a>`owner`: `riverhog_age.UploadState`
- <a id="s-6abbd9b1f0"></a>`unit`: `member`

### Declared structure

- <a id="s-04a342162a"></a>`kind`: `"classmethod"`
- <a id="s-a79e6864cd"></a>`signature`: `"\"(cls, data: 'bytes \| str') -> 'UploadState'\""`

## Maintained corroboration

### Related interface records

- [UploadState](riverhog-age-uploadstate.md)

## Governing policies

- <a id="pa-57b85946a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.UploadState.from_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 83c7569c3951f0efd2ed372f5b13c83a71971de74a6f514f5b0422de4efb8017 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, data: 'bytes | str') -> 'UploadState'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "from_json_bytes",
  "owner": "riverhog_age.UploadState",
  "unit": "member"
}
```

</details>
