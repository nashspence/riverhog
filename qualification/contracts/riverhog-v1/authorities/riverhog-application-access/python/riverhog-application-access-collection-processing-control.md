# riverhog_application_access.COLLECTION_PROCESSING_CONTROL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-pr-074d90a42f:d9bad97e90 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d490d141f9"></a>
- <a id="s-c41ba54eb7"></a>`distribution`: `riverhog-application-access`
- <a id="s-1f0c33e7dc"></a>`module`: `riverhog_application_access`
- <a id="s-dbe484e580"></a>`name`: `COLLECTION_PROCESSING_CONTROL`
- <a id="s-061ead3e3d"></a>`unit`: `export`

### Declared structure

- <a id="s-de3591a6b5"></a>`kind`: `"constant"`
- <a id="s-accc4aa8e8"></a>`value`: `"collection-processing:control"`

## Governing policies

- <a id="pa-850ea294c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_PROCESSING_CONTROL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18b092bb0b010a651cf88cf664bae6dce6a96b6cccf783c2d4d74b80fa217347 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-processing:control"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_PROCESSING_CONTROL",
  "unit": "export"
}
```

</details>
