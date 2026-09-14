# piggity retrieval cache

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache:39b3b1cbba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-f89b5b9d97"></a>Parser name: `cache`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-edb24bf96b"></a>`help` | <a id="s-a3dd0741f2"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-bcee1f1ce5"></a>`0` | <a id="s-e6175897dc"></a>`"noncontractual-framework-help"` | <a id="s-b466bb4498"></a>`"empty"` |

## Governing policies

- <a id="pa-06bd0b1e9d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/parameters`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/name`

<!-- exact-contract-value: c87a83e09642a735845aad980110537acda99378a410740ff0c4454499eea392 -->

```json
"cache"
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/retrieval/commands/cache/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

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
  }
]
```
