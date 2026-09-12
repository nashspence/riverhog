# piggity component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:piggity:piggity-component-boundary:43cd09ec8c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-de94abd7d4) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-97f233cab0"></a>
| Field | Shape |
|---|---|
| <a id="s-56090f65fb"></a>`console_scripts` | additional keys=`piggity` |
| <a id="s-1a6c7b1bcd"></a>`dependencies` | ["http-api-contracts","riverhog-application-access","riverhog-client","riverhog-protocol","riverhog-provenance","state-schema","time-formats"] |
| <a id="s-af8a4f6869"></a>`distribution` | "piggity" |
| <a id="s-b031a35a23"></a>`optional_dependencies` | empty object |
| <a id="s-742b19c000"></a>`path` | "reference/riverhog/applications/piggity" |
| <a id="s-361b39358b"></a>`role` | "reference_application" |

## Governing policies

- <a id="pa-6edfc52ce9"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/26`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 385c7e9cfbd8ded25aa1a4d6175f7d4ba15e76acbff592970dcc91cbeec0e0f1 -->

```json
{
  "console_scripts": {
    "piggity": "piggity.main:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-application-access",
    "riverhog-client",
    "riverhog-protocol",
    "riverhog-provenance",
    "state-schema",
    "time-formats"
  ],
  "distribution": "piggity",
  "optional_dependencies": {},
  "path": "reference/riverhog/applications/piggity",
  "role": "reference_application"
}
```
