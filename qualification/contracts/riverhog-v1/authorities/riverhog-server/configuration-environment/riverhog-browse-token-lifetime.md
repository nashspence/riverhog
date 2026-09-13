# RIVERHOG_BROWSE_TOKEN_LIFETIME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-server:riverhog-browse-token-lifetime:d0d653facb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-be3c1ab328"></a>
| Field | Shape |
|---|---|
| <a id="s-53a8b95257"></a>`consumers` | ["riverhog-server"] |
| <a id="s-92b359eb99"></a>`default_expressions` | ["'24h'"] |
| <a id="s-0695702476"></a>`id` | "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_LIFETIME" |
| <a id="s-204d487369"></a>`input_shape` | "environment-string" |
| <a id="s-e79d9952b9"></a>`name` | "RIVERHOG_BROWSE_TOKEN_LIFETIME" |
| <a id="s-5894963f53"></a>`owner` | "riverhog-server" |

## Governing policies

- <a id="pa-33daa7dfb0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_LIFETIME](../../../evidence/sources.md#src-9cc099a19b) — `riverhog/src/riverhog_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-server` | `riverhog/src/riverhog_core/runtime_config.py` | `os.getenv('RIVERHOG_BROWSE_TOKEN_LIFETIME', '24h')` |

### Machine authority

- `/external_contract/configuration_environment/49`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81577cd2105b1e6158434b704a6004fdede731d43ad35392e376ccdba8ffb719 -->

```json
{
  "consumers": [
    "riverhog-server"
  ],
  "default_expressions": [
    "'24h'"
  ],
  "id": "riverhog-server:environment:RIVERHOG_BROWSE_TOKEN_LIFETIME",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BROWSE_TOKEN_LIFETIME",
  "owner": "riverhog-server"
}
```
