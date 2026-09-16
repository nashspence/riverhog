# gogurt provider listener-host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-provider-listener-host:e069b9fd8c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-460e5802a5"></a>Parser name: `listener-host`

| Field | Value |
|---|---|
| <a id="s-5c86111e14"></a>`parameters` | `[]` |
- <a id="s-109eba08de"></a>Subcommand selection: required.
- <a id="s-575f9d1573"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-4f7ee5f37b"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-5db6a0b1c7"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-dc5a283764"></a>`help` | <a id="s-3fb69cb99a"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-c4f863a856"></a>`0` | <a id="s-4a6c97b22a"></a>`"noncontractual-framework-help"` | <a id="s-d470f176cc"></a>`"empty"` |

## Governing policies

- <a id="pa-10f0e66493"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:gogurt](../../../evidence/sources.md#src-3b2297c37d) — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/allow_extra_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/allow_interspersed_args`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/ignore_unknown_options`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/name`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/parameters`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/subcommand_required`
- `/external_contract/cli/gogurt/commands/provider/commands/listener-host/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/name`

<!-- exact-contract-value: 1ff192f1eb07f7c65d154f43a97e9ab9f7d5006efb66e5f85d9b7e24768038a9 -->

```json
"listener-host"
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/gogurt/commands/provider/commands/listener-host/terminating_controls`

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
