# riverhog_provenance.PROVENANCE_VOLUME_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-volume-schema:50032dc0f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0b5064c91"></a>
- <a id="s-8001710e06"></a>`distribution`: `riverhog-provenance`
- <a id="s-8f2aa21098"></a>`module`: `riverhog_provenance`
- <a id="s-afb1e28b62"></a>`name`: `PROVENANCE_VOLUME_SCHEMA`
- <a id="s-163572afdd"></a>`unit`: `export`

### Declared structure

- <a id="s-93d9fa0dc9"></a>`kind`: `"constant"`
- <a id="s-36cd534648"></a>`value`: `"riverhog-provenance-volume/v1"`

## Governing policies

- <a id="pa-7e35998525"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_VOLUME_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9de27eb24677ca5f3caeb985962fa55db572104b578fb862d09dc638e0770d50 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-volume/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_VOLUME_SCHEMA",
  "unit": "export"
}
```

</details>
