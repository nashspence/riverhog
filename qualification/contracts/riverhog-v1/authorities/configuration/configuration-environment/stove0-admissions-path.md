# STOVE0_ADMISSIONS_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-admissions-path:4cf1a4a644 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3efbdd0d504b"></a>
| Field | Shape |
|---|---|
| <a id="s-e5a845680150"></a>`consumers` | ["stove0-server"] |
| <a id="s-8bf9d9fca274"></a>`name` | "STOVE0_ADMISSIONS_PATH" |

## Governing policies

- <a id="pa-0e985d274fb3"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_ADMISSIONS_PATH](../../../evidence/sources.md#src-6962872e1dcd) — `configuration-environment:STOVE0_ADMISSIONS_PATH`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/78`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d647a63f9a08a2ebe093a1962bf59bb0cfba99865c06a718c3015fd02f683b8a -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_ADMISSIONS_PATH"
}
```
