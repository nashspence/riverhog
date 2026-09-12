# stove0-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-client:stove0-client-component-boundary:11bea8fac1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-c3f42d578a) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1fd100e55b"></a>
| Field | Shape |
|---|---|
| <a id="s-8ad421aae1"></a>`console_scripts` | additional keys=`stove0` |
| <a id="s-d8dfe6d928"></a>`dependencies` | ["stove0-api-client","stove0-protocol","stove0-recipe-config"] |
| <a id="s-9789c1083f"></a>`distribution` | "stove0-client" |
| <a id="s-fbd60e0677"></a>`optional_dependencies` | empty object |
| <a id="s-afcea98301"></a>`path` | "reference/stove0/application/client" |
| <a id="s-2e0116f69f"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-3f925fbfe4"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/40`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5050ad11e1faec9510d38db6cf5d9fe7106e65d8c58883b728a82e69afea1116 -->

```json
{
  "console_scripts": {
    "stove0": "stove0_cli.main:main"
  },
  "dependencies": [
    "stove0-api-client",
    "stove0-protocol",
    "stove0-recipe-config"
  ],
  "distribution": "stove0-client",
  "optional_dependencies": {},
  "path": "reference/stove0/application/client",
  "role": "reference_application"
}
```
