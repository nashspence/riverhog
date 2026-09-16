# riverhog_application_access.access_covers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-access-covers:bae25f8a81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-670a770c4d"></a>
- <a id="s-e56a1b0409"></a>`distribution`: `riverhog-application-access`
- <a id="s-d78ce452b8"></a>`module`: `riverhog_application_access`
- <a id="s-d59561a07d"></a>`name`: `access_covers`
- <a id="s-1ddaa1e711"></a>`unit`: `export`

### Declared structure

- <a id="s-c052f4fa8b"></a>`kind`: `"function"`
- <a id="s-db6b3f171c"></a>`signature`: `"\"(grantor: 'ApplicationAccess', requested: 'ApplicationAccess') -> 'bool'\""`

## Governing policies

- <a id="pa-f88a6ff85b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.access_covers`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acb02dcfab70072ff7b369a84afccca28cd49c26ead8ee982f7462e489319603 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(grantor: 'ApplicationAccess', requested: 'ApplicationAccess') -> 'bool'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "access_covers",
  "unit": "export"
}
```

</details>
