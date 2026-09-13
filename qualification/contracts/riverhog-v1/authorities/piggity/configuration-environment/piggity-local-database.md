# PIGGITY_LOCAL_DATABASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-local-database:0bc8709326 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-38bec3b929"></a>
| Field | Shape |
|---|---|
| <a id="s-9924516b14"></a>`consumers` | ["piggity"] |
| <a id="s-7d8f29784c"></a>`default_expressions` | ["''"] |
| <a id="s-1fe06e52b2"></a>`id` | "piggity:environment:PIGGITY_LOCAL_DATABASE" |
| <a id="s-32e5ce49f9"></a>`input_shape` | "environment-string" |
| <a id="s-a68f317075"></a>`name` | "PIGGITY_LOCAL_DATABASE" |
| <a id="s-db30e5af06"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-7fb49c701f"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_LOCAL_DATABASE](../../../evidence/sources.md#src-ab1d0074de) — `reference/riverhog/applications/piggity/src/piggity/local.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/local.py` | `os.getenv('PIGGITY_LOCAL_DATABASE', '')` |

### Machine authority

- `/external_contract/configuration_environment/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16371ca5995fcda5d42c1038ff085055b9e178b09cf8b74f84f74882029205a4 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "piggity:environment:PIGGITY_LOCAL_DATABASE",
  "input_shape": "environment-string",
  "name": "PIGGITY_LOCAL_DATABASE",
  "owner": "piggity"
}
```
