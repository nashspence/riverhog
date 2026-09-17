# riverhog_provenance.ProvenanceObserverBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceobserverbinding:83d51796d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c32b4a25dd"></a>
- <a id="s-ffb56a6616"></a>`distribution`: `riverhog-provenance`
- <a id="s-c3267644b8"></a>`module`: `riverhog_provenance`
- <a id="s-dd7c5440cf"></a>`name`: `ProvenanceObserverBinding`
- <a id="s-3b95ebc712"></a>`unit`: `export`

### Declared structure

- <a id="s-9d5b6fdfa2"></a>`kind`: `"class"`
- <a id="s-fdd8ab1793"></a>`signature`: `"\"(observer_id: 'str', contract_provider: 'str', contract_id: 'str', contract_sha256: 'str', factory: 'FileStateObserverFactory', format: 'str' = 'riverhog-provenance-observer-binding/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-14310170b8"></a>`observer_id` | `'str'` | `required` |
| <a id="s-4fc283e217"></a>`contract_provider` | `'str'` | `required` |
| <a id="s-ccbc87d875"></a>`contract_id` | `'str'` | `required` |
| <a id="s-81dff460ba"></a>`contract_sha256` | `'str'` | `required` |
| <a id="s-31f69e0e7f"></a>`factory` | `'FileStateObserverFactory'` | `required` |
| <a id="s-b7450d4abe"></a>`format` | `'str'` | `'riverhog-provenance-observer-binding/v1'` |

## Governing policies

- <a id="pa-7fdeb94fa2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceObserverBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a8ae4f2dc8719816e2796e9318e1d7561ae4441346097f0be433f0f66b0abb91 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "observer_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "contract_provider",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "contract_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "contract_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "factory",
        "type": "'FileStateObserverFactory'"
      },
      {
        "default": "'riverhog-provenance-observer-binding/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(observer_id: 'str', contract_provider: 'str', contract_id: 'str', contract_sha256: 'str', factory: 'FileStateObserverFactory', format: 'str' = 'riverhog-provenance-observer-binding/v1') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceObserverBinding",
  "unit": "export"
}
```

</details>
