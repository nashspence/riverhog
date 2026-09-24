# riverhog_application_access.COLLECTION_PROCESSING_EXECUTE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-pr-aeaaf3946c:fa9b358d19 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-347fec7caf"></a>
- <a id="s-3498342fce"></a>`distribution`: `riverhog-application-access`
- <a id="s-70d95f9602"></a>`module`: `riverhog_application_access`
- <a id="s-124e46ae79"></a>`name`: `COLLECTION_PROCESSING_EXECUTE`
- <a id="s-358a3a74a2"></a>`unit`: `export`

### Declared structure

- <a id="s-2affe5990f"></a>`kind`: `"constant"`
- <a id="s-f7be788b75"></a>`value`: `"collection-processing:execute"`

## Governing policies

- <a id="pa-7b6658e4c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_PROCESSING_EXECUTE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d483f54d01c329d39807df1188d8beee08ce61b4955bd0140fe6b22ad7455f5d -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-processing:execute"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_PROCESSING_EXECUTE",
  "unit": "export"
}
```

</details>
