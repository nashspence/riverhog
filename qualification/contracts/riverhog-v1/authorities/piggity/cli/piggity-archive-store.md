# piggity archive store

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-store:3b051dc1a2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1ba756a6de"></a>Parser name: `store`

| Field | Value |
|---|---|
| <a id="s-96003b7d34"></a>`parameters` | `[]` |
- <a id="s-6ee9aee256"></a>Subcommand selection: required.
- <a id="s-0cfc59afb8"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-3198317dea"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-6e4847f503"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-09484fa076"></a>`help` | <a id="s-bbb6503330"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-f79153c087"></a>`0` | <a id="s-6da09f2af5"></a>`"noncontractual-framework-help"` | <a id="s-b90e1b3e04"></a>`"empty"` |

## Governing policies

- <a id="pa-cc1f497c31"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/store/allow_extra_args`
- `/external_contract/cli/piggity/commands/archive/commands/store/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/archive/commands/store/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/archive/commands/store/name`
- `/external_contract/cli/piggity/commands/archive/commands/store/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/store/subcommand_required`
- `/external_contract/cli/piggity/commands/archive/commands/store/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/store/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/commands/store/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/store/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/commands/store/name`

<!-- exact-contract-value: 52b5e20f559958f34e533431143411e861c7ff8a5f1f57c13a15a6f319137dff -->

```json
"store"
```

### `/external_contract/cli/piggity/commands/archive/commands/store/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/archive/commands/store/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/commands/store/terminating_controls`

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
