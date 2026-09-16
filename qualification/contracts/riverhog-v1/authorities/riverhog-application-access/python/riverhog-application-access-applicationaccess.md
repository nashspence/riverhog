# riverhog_application_access.ApplicationAccess

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationaccess:579bcdc0a1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1a5baa7fd"></a>
- <a id="s-dd295d2924"></a>`distribution`: `riverhog-application-access`
- <a id="s-f79b9a47b8"></a>`module`: `riverhog_application_access`
- <a id="s-0049b8cfe6"></a>`name`: `ApplicationAccess`
- <a id="s-17fe8de5d7"></a>`unit`: `export`

### Declared structure

- <a id="s-5a6c73869d"></a>`kind`: `"class"`
- <a id="s-0415b15022"></a>`signature`: `"\"(permission: 'ApplicationPermission', resource: 'ApplicationResource' = '*') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-d85c5ca7a3"></a>`permission` | `'ApplicationPermission'` | `required` |
| <a id="s-7a4054370c"></a>`resource` | `'ApplicationResource'` | `'*'` |

## Governing policies

- <a id="pa-d3a74c1c6d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationAccess`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34333ff922d759bdb77e9f7019ec742d26af492f1567a71caff3b94df935f4c1 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "permission",
        "type": "'ApplicationPermission'"
      },
      {
        "default": "'*'",
        "name": "resource",
        "type": "'ApplicationResource'"
      }
    ],
    "kind": "class",
    "signature": "\"(permission: 'ApplicationPermission', resource: 'ApplicationResource' = '*') -> None\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationAccess",
  "unit": "export"
}
```

</details>
