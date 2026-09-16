# gogurt listener

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-listener:0ad38be5d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fab9428b9d"></a>Parser name: `listener`

| Field | Value |
|---|---|
| <a id="s-37e2fd2d9f"></a>`parameters` | `[]` |
- <a id="s-e8a1684f32"></a>Subcommand selection: required.
- <a id="s-46ce6dfceb"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-042ee91470"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-29e3de8f0a"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-e40059cd37"></a>`help` | <a id="s-8f3c5e4243"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-0398a327a5"></a>`0` | <a id="s-a41d082f85"></a>`"noncontractual-framework-help"` | <a id="s-84e437536d"></a>`"empty"` |

## Governing policies

- <a id="pa-1806686ddc"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/listener/allow_extra_args`
- `/external_contract/cli/gogurt/commands/listener/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/listener/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/listener/name`
- `/external_contract/cli/gogurt/commands/listener/parameters`
- `/external_contract/cli/gogurt/commands/listener/subcommand_required`
- `/external_contract/cli/gogurt/commands/listener/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/listener/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/listener/name`

<!-- exact-contract-value: aad4c0680220158304061172712a7c394609c4bc07492556ecad3cd69267821e -->

```json
"listener"
```

### `/external_contract/cli/gogurt/commands/listener/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/gogurt/commands/listener/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/listener/terminating_controls`

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

</details>
