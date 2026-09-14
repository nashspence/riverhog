# piggity local provenance-observer

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-provenance-observer:7bff0ce112 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-008acffa9c"></a>Parser name: `provenance-observer`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fff542a7ae"></a>`help` | <a id="s-936e878003"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-957b9b4c56"></a>`0` | <a id="s-eb7338fa94"></a>`"noncontractual-framework-help"` | <a id="s-46a6e46711"></a>`"empty"` |
| <a id="s-4fca9d200b"></a>`implicit-help` | <a id="s-3b3a7b2631"></a>`{"kind":"empty-invocation"}` | <a id="s-6f2ae25895"></a>`2` | <a id="s-0bc1e58543"></a>`"empty"` | <a id="s-865afadb72"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-7db385e56c"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/name`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/parameters`
- `/external_contract/cli/piggity/commands/local/commands/provenance-observer/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/name`

<!-- exact-contract-value: 9a274a23ecfd214275ad5e6ec65b7ddb001f01bdc958a7d41c955e9592ba5b40 -->

```json
"provenance-observer"
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/local/commands/provenance-observer/terminating_controls`

<!-- exact-contract-value: a96bedb6acb408986e7084c1d3216345e293f6c42987bb87a923c18807587f21 -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "--help"
      ]
    }
  },
  {
    "exit_status": 2,
    "id": "implicit-help",
    "stderr": "noncontractual-framework-help",
    "stdout": "empty",
    "trigger": {
      "kind": "empty-invocation"
    }
  }
]
```
