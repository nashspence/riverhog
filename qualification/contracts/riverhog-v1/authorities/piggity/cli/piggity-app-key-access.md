# piggity app key access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-access:94bddf1fba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-271021267b"></a>Parser name: `access`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-95a21412d4"></a>`help` | <a id="s-a65b168ec3"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-3a786309b8"></a>`0` | <a id="s-c2db648a9a"></a>`"noncontractual-framework-help"` | <a id="s-7cdfdec114"></a>`"empty"` |

## Governing policies

- <a id="pa-f20116971d"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/name`

<!-- exact-contract-value: 0fd85c57b5ffac7a9a2a06782aa152dd61b76bf505bb14103d40a3ae1ca4dae2 -->

```json
"access"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/terminating_controls`

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
