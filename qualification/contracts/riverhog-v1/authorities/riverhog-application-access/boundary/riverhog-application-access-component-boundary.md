# riverhog-application-access component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-application-access:riverhog-application-access-component-boundary:529bb88f14 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-6bd401ca9db0) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1dc6e85a96fa"></a>
| Field | Shape |
|---|---|
| <a id="s-feb156446bdc"></a>`console_scripts` | empty object |
| <a id="s-c54ca23d8d66"></a>`dependencies` | ["riverhog-protocol"] |
| <a id="s-43b680c1df37"></a>`distribution` | "riverhog-application-access" |
| <a id="s-8aec47b3df30"></a>`optional_dependencies` | empty object |
| <a id="s-ab89200e36a4"></a>`path` | "packages/riverhog-application-access" |
| <a id="s-f01c1b56dd54"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-ba705f4a3441"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/4`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51196e02687ed1bed81a81e73945f59d7e16e2dab709e6d9cfbef6b2b22a437d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "riverhog-application-access",
  "optional_dependencies": {},
  "path": "packages/riverhog-application-access",
  "role": "reusable_library"
}
```
