# STOVE0_OBSERVERS_JSON

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-observers-json:052705576f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e6ea24ca96"></a>

| Field | Value |
|---|---|
| <a id="s-bba0f0c4a2"></a>`consumers` | `["stove0-server"]` |
| <a id="s-891c749759"></a>`default_expressions` | `["'{}'"]` |
| <a id="s-5c7cfbb798"></a>`id` | `"stove0-server:environment:STOVE0_OBSERVERS_JSON"` |
| <a id="s-dd7740d2f8"></a>`input_shape` | `"environment-string"` |
| <a id="s-35af37b988"></a>`name` | `"STOVE0_OBSERVERS_JSON"` |
| <a id="s-bee3607ec5"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-8dab131f68"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_OBSERVERS_JSON](../../../evidence/sources.md#src-5e770d1e0f) — `reference/stove0/application/server/src/stove0_core/runtime_config.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | `reference/stove0/application/server/src/stove0_core/runtime_config.py` | `values.get(name, '{}')` |

### Machine authority

- `/external_contract/configuration_environment/238`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e3ef4e08911ff6cb2820f17986e7dfb33d01e02e059590267b12187de9065ff -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "'{}'"
  ],
  "id": "stove0-server:environment:STOVE0_OBSERVERS_JSON",
  "input_shape": "environment-string",
  "name": "STOVE0_OBSERVERS_JSON",
  "owner": "stove0-server"
}
```

</details>
