# riverhog-ftp-adapter component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter:riverhog-ftp-adapter-component-boundary:33f5955bfe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-391701f840) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6f624af912"></a>
| Field | Shape |
|---|---|
| <a id="s-41ee0b2f8c"></a>`console_scripts` | additional keys=`riverhog-ftp-adapter` |
| <a id="s-2e85a65261"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-ftp-adapter-api-client","riverhog-protocol","riverhog-provenance"] |
| <a id="s-f3a66f6d63"></a>`distribution` | "riverhog-ftp-adapter" |
| <a id="s-e2ec39398d"></a>`optional_dependencies` | empty object |
| <a id="s-91d6b15ef1"></a>`path` | "reference/riverhog/ingress/ftp" |
| <a id="s-fd2ac2b9d9"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-45fc25abdb"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/27`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59d9e4097d12863a20e0d3c3d2fff5e7c2253b4dc1e0cdebce56e74f8bb883af -->

```json
{
  "console_scripts": {
    "riverhog-ftp-adapter": "riverhog_ftp_adapter.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-ftp-adapter-api-client",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-ftp-adapter",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp",
  "role": "reference_component"
}
```
