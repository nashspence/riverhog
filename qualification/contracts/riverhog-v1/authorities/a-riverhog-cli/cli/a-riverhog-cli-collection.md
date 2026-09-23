# a-riverhog-cli collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-collection:b046303499 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d0c5fbcffb"></a>Parser name: `collection`

| Field | Value |
|---|---|
| <a id="s-e24c4b1436"></a>`parameters` | `[]` |
- <a id="s-707a0fcd9b"></a>Subcommand selection: required.
- <a id="s-aea6deff94"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-de374cc93b"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-1a0b0975e4"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-745992f2b3"></a>`help` | <a id="s-ca58942718"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-6abc4b10d0"></a>`0` | <a id="s-bcc8385dda"></a>`"noncontractual-framework-help"` | <a id="s-93001c7cb3"></a>`"empty"` |

## Governing policies

- <a id="pa-ec00c05b75"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/collection/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/collection/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/collection/name`
- `/external_contract/cli/a-riverhog-cli/commands/collection/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/collection/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/collection/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/collection/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/name`

<!-- exact-contract-value: fe934a0dca42a0d8d4199da38ce35d5d8c19e63f659963a6791b82c13494b0a0 -->

```json
"collection"
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/collection/terminating_controls`

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
