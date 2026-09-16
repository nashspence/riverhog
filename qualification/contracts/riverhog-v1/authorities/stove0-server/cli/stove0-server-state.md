# stove0-server state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-server:stove0-server-state:f0f444a0cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3143e65bdd"></a>Parser name: `state`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6cdc4ea8e5"></a>`help` | <a id="s-ddfdd7c0dd"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f4100d02a9"></a>`0` | <a id="s-d3d1c60aa1"></a>`"noncontractual-framework-help"` | <a id="s-2ff2f4073b"></a>`"empty"` |

## Governing policies

- <a id="pa-f25edf361a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0-server](../../../evidence/sources.md#src-6f5bc9f6db) — `reference/stove0/application/server/src/stove0_api/app.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0-server/commands/state/name`
- `/external_contract/cli/stove0-server/commands/state/parameters`
- `/external_contract/cli/stove0-server/commands/state/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0-server/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/stove0-server/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0-server/commands/state/terminating_controls`

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
