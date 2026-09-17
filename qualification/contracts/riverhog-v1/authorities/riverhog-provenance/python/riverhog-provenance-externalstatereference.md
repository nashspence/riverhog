# riverhog_provenance.ExternalStateReference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-externalstatereference:a9f099e64f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b623c3ac9"></a>
- <a id="s-21deabb02c"></a>`distribution`: `riverhog-provenance`
- <a id="s-6248b188c7"></a>`module`: `riverhog_provenance`
- <a id="s-be08584cb1"></a>`name`: `ExternalStateReference`
- <a id="s-18c91c103d"></a>`unit`: `export`

### Declared structure

- <a id="s-15bc04f494"></a>`kind`: `"class"`
- <a id="s-e7354317cc"></a>`signature`: `"\"(journal_id: 'str', entry_id: 'str', entry_json_sha256: 'str', state_id: 'str') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-7f4bd51c6d"></a>`journal_id` | `'str'` | `required` |
| <a id="s-5cc239c8cc"></a>`entry_id` | `'str'` | `required` |
| <a id="s-b2b0188238"></a>`entry_json_sha256` | `'str'` | `required` |
| <a id="s-d82efb6524"></a>`state_id` | `'str'` | `required` |

## Governing policies

- <a id="pa-b7dae286a6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ExternalStateReference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61c5ca76e414f51cd27094e712d78b146d77c8833df03768dfcd95fd3c6c815d -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "journal_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "entry_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "entry_json_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "state_id",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(journal_id: 'str', entry_id: 'str', entry_json_sha256: 'str', state_id: 'str') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ExternalStateReference",
  "unit": "export"
}
```

</details>
