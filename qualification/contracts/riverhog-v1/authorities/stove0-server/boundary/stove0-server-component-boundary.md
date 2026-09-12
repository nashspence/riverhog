# stove0-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-server:stove0-server-component-boundary:151ebfc28c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1e6fdf7989) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eadeab01de"></a>
| Field | Shape |
|---|---|
| <a id="s-e7d0db8c98"></a>`console_scripts` | additional keys=`stove0-server` |
| <a id="s-19cb982dd7"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","state-schema","stove0-observer-client","stove0-observer-protocol","stove0-operator-contracts","stove0-protocol","stove0-recipe-config","stove0-target-client","stove0-target-protocol","time-formats"] |
| <a id="s-667c0d191d"></a>`distribution` | "stove0-server" |
| <a id="s-c873ef2061"></a>`optional_dependencies` | empty object |
| <a id="s-70a8a8fd00"></a>`path` | "reference/stove0/application/server" |
| <a id="s-a9b7a33cd5"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-982cb06b52"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52133f596bcaf6efb19342c532476dae5e76936add38866137ba52ffd3dbb3c7 -->

```json
{
  "console_scripts": {
    "stove0-server": "stove0_api.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "state-schema",
    "stove0-observer-client",
    "stove0-observer-protocol",
    "stove0-operator-contracts",
    "stove0-protocol",
    "stove0-recipe-config",
    "stove0-target-client",
    "stove0-target-protocol",
    "time-formats"
  ],
  "distribution": "stove0-server",
  "optional_dependencies": {},
  "path": "reference/stove0/application/server",
  "role": "reference_application"
}
```
