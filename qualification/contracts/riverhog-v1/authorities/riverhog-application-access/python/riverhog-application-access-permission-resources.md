# riverhog_application_access.permission_resources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-permission-resources:15bed5ff65 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-35d49616fa"></a>
- <a id="s-4d4cb35482"></a>`distribution`: `riverhog-application-access`
- <a id="s-528291ae68"></a>`module`: `riverhog_application_access`
- <a id="s-c618c9b686"></a>`name`: `permission_resources`
- <a id="s-e7bdc9a193"></a>`unit`: `export`

### Declared structure

- <a id="s-c9fd76e2d0"></a>`kind`: `"function"`
- <a id="s-eaa5f75aeb"></a>`signature`: `"\"(access: 'Iterable[ApplicationAccess]', permission: 'str') -> 'set[str]'\""`

## Governing policies

- <a id="pa-c7dfb56cef"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.permission_resources`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 14225c5f158d5c662441dfe0a1a039f277a28e960b918bb6be68ce2e67d8bb4f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(access: 'Iterable[ApplicationAccess]', permission: 'str') -> 'set[str]'\""
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "permission_resources",
  "unit": "export"
}
```

</details>
