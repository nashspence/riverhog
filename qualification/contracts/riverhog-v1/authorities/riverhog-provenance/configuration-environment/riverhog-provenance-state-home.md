# RIVERHOG_PROVENANCE_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-provenance:riverhog-provenance-state-home:229b57c990 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-f77147b1c3"></a>
| Field | Shape |
|---|---|
| <a id="s-4504ac9a24"></a>`consumers` | ["riverhog-provenance"] |
| <a id="s-dd23c689a9"></a>`default_expressions` | ["unset"] |
| <a id="s-d6c9861ab7"></a>`id` | "riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME" |
| <a id="s-327d2c8e3c"></a>`input_shape` | "environment-string" |
| <a id="s-8af67c498b"></a>`name` | "RIVERHOG_PROVENANCE_STATE_HOME" |
| <a id="s-e1481d42fc"></a>`owner` | "riverhog-provenance" |

## Governing policies

- <a id="pa-b99a0f6609"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-provenance:RIVERHOG_PROVENANCE_STATE_HOME](../../../evidence/sources.md#src-9fe04e9d42) — `packages/riverhog-provenance/src/riverhog_provenance/identity.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-provenance` | `packages/riverhog-provenance/src/riverhog_provenance/identity.py` | `os.getenv('RIVERHOG_PROVENANCE_STATE_HOME')` |

### Machine authority

- `/external_contract/configuration_environment/33`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 216534df74d09e478d44ab6b623cb2fadd540d11dffe4684dd4be34ff6563d8f -->

```json
{
  "consumers": [
    "riverhog-provenance"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "riverhog-provenance:environment:RIVERHOG_PROVENANCE_STATE_HOME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_PROVENANCE_STATE_HOME",
  "owner": "riverhog-provenance"
}
```
