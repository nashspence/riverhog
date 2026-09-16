# riverhog_application_access.QUOTAS_MANAGE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-quotas-manage:1c778994c9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a2a41d36f"></a>
- <a id="s-540bc339e5"></a>`distribution`: `riverhog-application-access`
- <a id="s-076edfed87"></a>`module`: `riverhog_application_access`
- <a id="s-8ac15abcc9"></a>`name`: `QUOTAS_MANAGE`
- <a id="s-c223fa8b72"></a>`unit`: `export`

### Declared structure

- <a id="s-5fe2b9baa3"></a>`kind`: `"constant"`
- <a id="s-2ba3484335"></a>`value`: `"quotas:manage"`

## Governing policies

- <a id="pa-102550cf77"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.QUOTAS_MANAGE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07eac405943033c7172cf18fd5734d5cd18e07eeb77d99037b18463e77db0142 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "quotas:manage"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "QUOTAS_MANAGE",
  "unit": "export"
}
```

</details>
