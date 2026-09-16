# riverhog_application_access.COLLECTIONS_DELETE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-collections-delete:cc18092bcf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0ed152b3d6"></a>
- <a id="s-74d2456177"></a>`distribution`: `riverhog-application-access`
- <a id="s-806ea82f7e"></a>`module`: `riverhog_application_access`
- <a id="s-532522a92d"></a>`name`: `COLLECTIONS_DELETE`
- <a id="s-04aa01ea80"></a>`unit`: `export`

### Declared structure

- <a id="s-d9e333add5"></a>`kind`: `"constant"`
- <a id="s-f990d42a76"></a>`value`: `"collections:delete"`

## Governing policies

- <a id="pa-0052163284"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.COLLECTIONS_DELETE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0eeb7b6994f2765247224e293d834e325db32d9e2758472d178ae9502f6734ad -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collections:delete"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "COLLECTIONS_DELETE",
  "unit": "export"
}
```

</details>
