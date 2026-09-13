# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:riverhog-ftp-adapter:riverhog-allow-insecure-http:ceb4ceebd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-da42edbdae"></a>
| Field | Shape |
|---|---|
| <a id="s-cb8b3d3b8e"></a>`consumers` | ["riverhog-ftp-adapter"] |
| <a id="s-24d8864685"></a>`default_expressions` | ["'false'"] |
| <a id="s-c407419be0"></a>`id` | "riverhog-ftp-adapter:environment:RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-9d5b6c09f0"></a>`input_shape` | "environment-string" |
| <a id="s-c08568c77a"></a>`name` | "RIVERHOG_ALLOW_INSECURE_HTTP" |
| <a id="s-bf0770eba1"></a>`owner` | "riverhog-ftp-adapter" |

## Governing policies

- <a id="pa-1bcca6bc7b"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:riverhog-ftp-adapter:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-8be0f96180) — `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `riverhog-ftp-adapter` | `reference/riverhog/ingress/ftp/src/riverhog_ftp_adapter/config.py` | `os.environ.get('RIVERHOG_ALLOW_INSECURE_HTTP', 'false')` |

### Machine authority

- `/external_contract/configuration_environment/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e235ed8b0d5c5f2e0a2501ce3616478210d5b434cc3b6205d48587cc2eb070de -->

```json
{
  "consumers": [
    "riverhog-ftp-adapter"
  ],
  "default_expressions": [
    "'false'"
  ],
  "id": "riverhog-ftp-adapter:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "riverhog-ftp-adapter"
}
```
