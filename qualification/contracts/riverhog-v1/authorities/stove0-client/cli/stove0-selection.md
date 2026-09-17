# stove0 selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-selection:2cf922e33a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6bffd50306"></a>Parser name: `selection`

| Field | Value |
|---|---|
| <a id="s-edeea8a1e9"></a>`parameters` | `[]` |
- <a id="s-636676efa6"></a>Subcommand selection: required.
- <a id="s-2d545ece7a"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-4cab6297ca"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-e852ca2be1"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7e964ecbb9"></a>`help` | <a id="s-5a7cc4df40"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5a713070da"></a>`0` | <a id="s-ff556c34b4"></a>`"noncontractual-framework-help"` | <a id="s-2be7d9f11c"></a>`"empty"` |

## Governing policies

- <a id="pa-56e8b3e31e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/stove0/commands/selection/allow_extra_args`
- `/external_contract/cli/stove0/commands/selection/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/selection/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/selection/name`
- `/external_contract/cli/stove0/commands/selection/parameters`
- `/external_contract/cli/stove0/commands/selection/subcommand_required`
- `/external_contract/cli/stove0/commands/selection/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/selection/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/selection/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/selection/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/selection/name`

<!-- exact-contract-value: 5d7642cc69080cf161bddfd3b46d6188f43e9b7d7c2bc8aeb1ef833afac83214 -->

```json
"selection"
```

### `/external_contract/cli/stove0/commands/selection/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/selection/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/selection/terminating_controls`

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
