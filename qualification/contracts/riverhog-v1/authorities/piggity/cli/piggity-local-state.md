# piggity local state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-local-state:fcc6bd6101 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1a8b32ba1e"></a>Parser name: `state`
- <a id="s-cc996c6442"></a>Subcommand selection: required.
- <a id="s-764b0b84eb"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-2ed9ba3b71"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-30aebc253e"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-26ffd89bcc"></a>`help` | <a id="s-a5e6da9541"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-0dfea6048d"></a>`0` | <a id="s-ca44c36595"></a>`"noncontractual-framework-help"` | <a id="s-dca74cbc82"></a>`"empty"` |
| <a id="s-a6472af674"></a>`implicit-help` | <a id="s-d94f8ced5c"></a>`{"kind":"empty-invocation"}` | <a id="s-6a8e359ef9"></a>`2` | <a id="s-c8718b10d4"></a>`"empty"` | <a id="s-514758810e"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-2fed7467de"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/local/commands/state/allow_extra_args`
- `/external_contract/cli/piggity/commands/local/commands/state/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/local/commands/state/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/local/commands/state/name`
- `/external_contract/cli/piggity/commands/local/commands/state/parameters`
- `/external_contract/cli/piggity/commands/local/commands/state/subcommand_required`
- `/external_contract/cli/piggity/commands/local/commands/state/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/local/commands/state/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/state/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/state/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/local/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/piggity/commands/local/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/local/commands/state/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/local/commands/state/terminating_controls`

<!-- exact-contract-value: a96bedb6acb408986e7084c1d3216345e293f6c42987bb87a923c18807587f21 -->

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
  },
  {
    "exit_status": 2,
    "id": "implicit-help",
    "stderr": "noncontractual-framework-help",
    "stdout": "empty",
    "trigger": {
      "kind": "empty-invocation"
    }
  }
]
```

</details>
