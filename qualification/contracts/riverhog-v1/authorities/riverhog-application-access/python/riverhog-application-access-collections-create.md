# riverhog_application_access.COLLECTIONS_CREATE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collections-create:5033236e9f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cbe1846bd6"></a>
- <a id="s-679b8b943a"></a>`distribution`: `riverhog-application-access`
- <a id="s-cf723a2cfd"></a>`module`: `riverhog_application_access`
- <a id="s-6acf72e89c"></a>`name`: `COLLECTIONS_CREATE`
- <a id="s-a600efbc9b"></a>`unit`: `export`

### Declared structure

- <a id="s-84d1512ce4"></a>`kind`: `"constant"`
- <a id="s-707c28836c"></a>`value`: `"collections:create"`

## Governing policies

- <a id="pa-9ba1b29e65"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTIONS_CREATE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e526f5f2dfb21fefefb2d32c903b3bafb5f9e74771d3415df7bc117a523bacc -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collections:create"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTIONS_CREATE",
  "unit": "export"
}
```

</details>
