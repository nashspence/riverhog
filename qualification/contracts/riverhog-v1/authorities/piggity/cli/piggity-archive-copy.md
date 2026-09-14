# piggity archive copy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy:c314b0c373 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dd93faf20b"></a>Parser name: `copy`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cd947e4f09"></a>`help` | <a id="s-b2ce35ce1b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-90fe9ca94a"></a>`0` | <a id="s-e481bb0490"></a>`"noncontractual-framework-help"` | <a id="s-5b5dc06928"></a>`"empty"` |

## Governing policies

- <a id="pa-c7774b17e1"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/copy/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/name`

<!-- exact-contract-value: 166b0224642d899b3bcee9662565384be54c79a393bef30d262bfd1eed1d5a97 -->

```json
"copy"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/terminating_controls`

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
