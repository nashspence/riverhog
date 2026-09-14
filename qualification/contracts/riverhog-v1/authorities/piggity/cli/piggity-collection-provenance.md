# piggity collection provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-provenance:c10fb4ec16 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1247d0a244"></a>Parser name: `provenance`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-949d84f78f"></a>`help` | <a id="s-5a57731387"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b1a78589ee"></a>`0` | <a id="s-5e761b7154"></a>`"noncontractual-framework-help"` | <a id="s-076ef710b7"></a>`"empty"` |

## Governing policies

- <a id="pa-7878361448"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/provenance/name`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/provenance/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/provenance/name`

<!-- exact-contract-value: 177887b1fb536becb2829efbdfcb7f7b2c801f57dd46da7319fa7d5bf9838bb1 -->

```json
"provenance"
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/collection/commands/provenance/terminating_controls`

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
