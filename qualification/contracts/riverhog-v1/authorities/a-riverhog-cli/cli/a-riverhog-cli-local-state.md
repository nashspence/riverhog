# a-riverhog-cli local state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-local-state:94ae493fe9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b2a56b72b5"></a>Parser name: `state`

| Field | Value |
|---|---|
| <a id="s-ec30a26c66"></a>`parameters` | `[]` |
- <a id="s-9b4200b4da"></a>Subcommand selection: required.
- <a id="s-c62e230d16"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-4a44dd0787"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-1c6f669f61"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-11aa291184"></a>`help` | <a id="s-cca394b7f7"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-6e97d7ac96"></a>`0` | <a id="s-a37a746cde"></a>`"noncontractual-framework-help"` | <a id="s-06dfafc825"></a>`"empty"` |
| <a id="s-750d421fbe"></a>`implicit-help` | <a id="s-6409d657c1"></a>`{"kind":"empty-invocation"}` | <a id="s-aa715062f5"></a>`2` | <a id="s-0487daf62f"></a>`"empty"` | <a id="s-a89057dd0c"></a>`"noncontractual-framework-help"` |

## Governing policies

- <a id="pa-b4d61b4297"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/name`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/name`

<!-- exact-contract-value: 55af5b1d18b0d9ddda33273d987fd0579ef9b07af3a077c79d0accb6c8f17127 -->

```json
"state"
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/local/commands/state/terminating_controls`

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
