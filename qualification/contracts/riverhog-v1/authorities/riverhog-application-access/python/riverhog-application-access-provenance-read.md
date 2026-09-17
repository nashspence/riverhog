# riverhog_application_access.PROVENANCE_READ

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-provenance-read:cace9c5467 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0014c4a8ed"></a>
- <a id="s-fb5a342d8b"></a>`distribution`: `riverhog-application-access`
- <a id="s-ca00633e8d"></a>`module`: `riverhog_application_access`
- <a id="s-d813e7af54"></a>`name`: `PROVENANCE_READ`
- <a id="s-619fe0f010"></a>`unit`: `export`

### Declared structure

- <a id="s-ff1e76b24c"></a>`kind`: `"constant"`
- <a id="s-ac061f991b"></a>`value`: `"provenance:read"`

## Governing policies

- <a id="pa-9745266260"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — [packages/riverhog-application-access/src/riverhog\_application\_access/\_\_init\_\_.py](../../../../../../packages/riverhog-application-access/src/riverhog_application_access/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_application_access.PROVENANCE_READ`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2271a0afe94ee1629c31287cfccf4a97a1fb933c3ebbbbe30a7b12bfa165bf0f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "provenance:read"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "PROVENANCE_READ",
  "unit": "export"
}
```

</details>
