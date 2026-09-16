# riverhog_application_access.APPLICATION_NAME_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-application-name-pattern:884d743647 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39d193641d"></a>
- <a id="s-8947a63de2"></a>`distribution`: `riverhog-application-access`
- <a id="s-3b32817e5e"></a>`module`: `riverhog_application_access`
- <a id="s-0bdeccf4a6"></a>`name`: `APPLICATION_NAME_PATTERN`
- <a id="s-1f7286226d"></a>`unit`: `export`

### Declared structure

- <a id="s-9dea278cfb"></a>`kind`: `"constant"`
- <a id="s-b6565b90a3"></a>`value`: `"^[a-z0-9]+(?:-[a-z0-9]+)*$"`

## Governing policies

- <a id="pa-a5e94390f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.APPLICATION_NAME_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13200b25f2aa8a3f35aaa5049de0fac2bcf047c12301c25a1635973267bcae47 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[a-z0-9]+(?:-[a-z0-9]+)*$"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "APPLICATION_NAME_PATTERN",
  "unit": "export"
}
```

</details>
