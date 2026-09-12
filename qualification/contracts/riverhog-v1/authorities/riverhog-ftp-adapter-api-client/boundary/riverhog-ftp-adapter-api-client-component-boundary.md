# riverhog-ftp-adapter-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-component-boundary:080243d900 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-c2a04fb000) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-64ea3d7a10"></a>
| Field | Shape |
|---|---|
| <a id="s-ba86c25979"></a>`console_scripts` | empty object |
| <a id="s-5f7292ce69"></a>`dependencies` | ["http-api-contracts"] |
| <a id="s-34bd9d8a2f"></a>`distribution` | "riverhog-ftp-adapter-api-client" |
| <a id="s-0b18979622"></a>`optional_dependencies` | empty object |
| <a id="s-db0b9a026d"></a>`path` | "reference/riverhog/ingress/ftp-api-client" |
| <a id="s-4ad06bdff8"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-e7b8ef605b"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/28`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32f59f147ed4efabd494b32e3cfc2e7a7776a4e5fe4cc96549f9f100a90fb5f5 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts"
  ],
  "distribution": "riverhog-ftp-adapter-api-client",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp-api-client",
  "role": "reference_component"
}
```
