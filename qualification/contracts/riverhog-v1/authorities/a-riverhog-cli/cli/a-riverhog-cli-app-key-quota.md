# a-riverhog-cli app key quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-app-key-quota:ed34fc1d18 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2a26dbb179"></a>Parser name: `quota`

| Field | Value |
|---|---|
| <a id="s-bf04bf1fb6"></a>`parameters` | `[]` |
- <a id="s-5c3402e819"></a>Subcommand selection: required.
- <a id="s-6b8b40adf9"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-55575729bf"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-ca5a5f0057"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-cd531ca099"></a>`help` | <a id="s-75f829bda6"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-5a4c726c63"></a>`0` | <a id="s-599d34b90a"></a>`"noncontractual-framework-help"` | <a id="s-960c68c5ff"></a>`"empty"` |

## Governing policies

- <a id="pa-d3a16dd918"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/name`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/name`

<!-- exact-contract-value: 3360ca6f13b270b0fe102fa2aa8f51c110a79196ce8d27dcb14399b7dd3f4178 -->

```json
"quota"
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/app/commands/key/commands/quota/terminating_controls`

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
