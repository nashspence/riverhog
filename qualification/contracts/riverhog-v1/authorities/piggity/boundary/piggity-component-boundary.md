# piggity component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:piggity:piggity-component-boundary:43cd09ec8c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`piggity` |
| `dependencies` | ["http-api-contracts","riverhog-application-access","riverhog-client","riverhog-protocol","riverhog-provenance","state-schema","time-formats"] |
| `distribution` | "piggity" |
| `optional_dependencies` | empty object |
| `path` | "reference/riverhog/applications/piggity" |
| `role` | "reference_application" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

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
