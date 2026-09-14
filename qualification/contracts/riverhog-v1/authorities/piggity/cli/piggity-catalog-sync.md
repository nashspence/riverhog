# piggity catalog-sync

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync:5a97e3a4cf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8f6743b694"></a>Parser name: `catalog-sync`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-1b35d3d154"></a>`help` | <a id="s-b8d5538ada"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-2682b4fb53"></a>`0` | <a id="s-a52d540a1b"></a>`"noncontractual-framework-help"` | <a id="s-0bae674edd"></a>`"empty"` |

## Governing policies

- <a id="pa-1e5846799b"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/name`
- `/external_contract/cli/piggity/commands/catalog-sync/parameters`
- `/external_contract/cli/piggity/commands/catalog-sync/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/name`

<!-- exact-contract-value: 7419f7b042fd729e9a5868782776bde4fedb6a4004d2a2e5b88a4e81f6016b05 -->

```json
"catalog-sync"
```

### `/external_contract/cli/piggity/commands/catalog-sync/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/catalog-sync/terminating_controls`

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
