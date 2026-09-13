# PIGGITY_LOCAL_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:piggity:piggity-local-root:48148cab33 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-70ab20d602"></a>
| Field | Shape |
|---|---|
| <a id="s-cf5ca173ed"></a>`consumers` | ["piggity"] |
| <a id="s-ba39d61b87"></a>`default_expressions` | ["''"] |
| <a id="s-3b7a45f265"></a>`id` | "piggity:environment:PIGGITY_LOCAL_ROOT" |
| <a id="s-0cfd103afc"></a>`input_shape` | "environment-string" |
| <a id="s-e26009ba61"></a>`name` | "PIGGITY_LOCAL_ROOT" |
| <a id="s-ba143c7f61"></a>`owner` | "piggity" |

## Governing policies

- <a id="pa-3471f05d94"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:piggity:PIGGITY_LOCAL_ROOT](../../../evidence/sources.md#src-7c4314c2cb) — `reference/riverhog/applications/piggity/src/piggity/local.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `piggity` | `reference/riverhog/applications/piggity/src/piggity/local.py` | `os.getenv('PIGGITY_LOCAL_ROOT', '')` |

### Machine authority

- `/external_contract/configuration_environment/6`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a3db9f691d77c059197085f0e155b0c0412bb63df1d186355ba4bbd400139e9 -->

```json
{
  "consumers": [
    "piggity"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "piggity:environment:PIGGITY_LOCAL_ROOT",
  "input_shape": "environment-string",
  "name": "PIGGITY_LOCAL_ROOT",
  "owner": "piggity"
}
```
