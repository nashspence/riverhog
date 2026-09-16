# riverhog_application_access.TAG_PREFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-tag-prefix:2ca9f086b2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58c2b31b16"></a>
- <a id="s-d7743eb8ed"></a>`distribution`: `riverhog-application-access`
- <a id="s-d60930703d"></a>`module`: `riverhog_application_access`
- <a id="s-32a6146dcd"></a>`name`: `TAG_PREFIX`
- <a id="s-5250488521"></a>`unit`: `export`

### Declared structure

- <a id="s-5e86e7cbcb"></a>`kind`: `"constant"`
- <a id="s-21dad233c9"></a>`value`: `"tag:"`

## Governing policies

- <a id="pa-471c047cd7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.TAG_PREFIX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94563619833938d1730b83142fb9c2184a66d59fe29ad3a65a122a3ebed2a099 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "tag:"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "TAG_PREFIX",
  "unit": "export"
}
```

</details>
