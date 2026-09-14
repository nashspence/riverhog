# stove0 admission policy

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-policy:282b3b335b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-639a182acc"></a>Parser name: `policy`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cc1e89b440"></a>`help` | <a id="s-ea5d8075f1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-4658a37dd6"></a>`0` | <a id="s-d535c00c41"></a>`"noncontractual-framework-help"` | <a id="s-da782f72c6"></a>`"empty"` |

## Governing policies

- <a id="pa-c7e138a2fe"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/policy/name`
- `/external_contract/cli/stove0/commands/admission/commands/policy/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/policy/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/policy/name`

<!-- exact-contract-value: 17ad92e63c962393c0329c658937d16eccaea13036412a3d1d0a5b6b8f29d738 -->

```json
"policy"
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/admission/commands/policy/terminating_controls`

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
