# mango-fish state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state:14ba51b925 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e35963c48c"></a>Parser name: `state`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5ed0f88133"></a>`help` | <a id="s-cd49a21ef7"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-acbc175b8a"></a>`0` | <a id="s-2b0bfb109a"></a>`"noncontractual-framework-help"` | <a id="s-72f9649381"></a>`"empty"` |

## Governing policies

- <a id="pa-2d21f604c2"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources.md#src-3dcd5eedf2) — `reference/riverhog/applications/mango-fish/src/mango_fish/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/name`
- `/external_contract/cli/mango-fish/commands/state/parameters`
- `/external_contract/cli/mango-fish/commands/state/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/mango-fish/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/mango-fish/commands/state/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
        "-h",
        "--help"
      ]
    }
  }
]
```
