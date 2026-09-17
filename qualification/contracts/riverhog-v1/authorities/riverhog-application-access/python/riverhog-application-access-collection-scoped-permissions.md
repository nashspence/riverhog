# riverhog_application_access.COLLECTION_SCOPED_PERMISSIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collection-sc-fb32572c44:4b67bed5a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-472c621fb0"></a>
- <a id="s-57f3f441f6"></a>`distribution`: `riverhog-application-access`
- <a id="s-8d74bbe69d"></a>`module`: `riverhog_application_access`
- <a id="s-79aae02aee"></a>`name`: `COLLECTION_SCOPED_PERMISSIONS`
- <a id="s-ff879e75bf"></a>`unit`: `export`

### Declared structure

- <a id="s-e6ec8c88b0"></a>`kind`: `"constant"`
- <a id="s-99a19c4ecf"></a>`value`: `["archives:manage","archives:read","catalog:read","collection-descriptions:manage","collection-tags:manage","collections:delete","provenance:export","provenance:read","retrieval:manage"]`

## Governing policies

- <a id="pa-9f17b1ea9e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources/authorities.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTION_SCOPED_PERMISSIONS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57bcd045563d05e8a9938843bc72b9e5692e16152f2e9de8aca64455d2e06bd9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "archives:manage",
      "archives:read",
      "catalog:read",
      "collection-descriptions:manage",
      "collection-tags:manage",
      "collections:delete",
      "provenance:export",
      "provenance:read",
      "retrieval:manage"
    ]
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTION_SCOPED_PERMISSIONS",
  "unit": "export"
}
```

</details>
