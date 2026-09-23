# a-riverhog-cli catalog-sync

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-cli:a-riverhog-cli-catalog-sync:c80bc284aa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-a719a65036"></a>Parser name: `catalog-sync`

| Field | Value |
|---|---|
| <a id="s-faf5c4c7dc"></a>`parameters` | `[]` |
- <a id="s-7a8eb5cb10"></a>Subcommand selection: required.
- <a id="s-32e4b2e367"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-3b1ef8078f"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-39a96d7561"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ced8c840bf"></a>`help` | <a id="s-2446313bbf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a4ae684999"></a>`0` | <a id="s-4704f925f9"></a>`"noncontractual-framework-help"` | <a id="s-a346563618"></a>`"empty"` |

## Governing policies

- <a id="pa-8e74364d6e"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-cli](../../../evidence/sources/authorities.md#src-d2d8219a30) — [some-implementations/riverhog/applications/a-riverhog-cli/src/a\_riverhog\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/allow_extra_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/allow_interspersed_args`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/ignore_unknown_options`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/name`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/parameters`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/subcommand_required`
- `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/name`

<!-- exact-contract-value: 7419f7b042fd729e9a5868782776bde4fedb6a4004d2a2e5b88a4e81f6016b05 -->

```json
"catalog-sync"
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-cli/commands/catalog-sync/terminating_controls`

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
