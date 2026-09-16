# piggity retrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval:3e94580a37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d842536a49"></a>Parser name: `retrieval`

| Field | Value |
|---|---|
| <a id="s-ecc4cd8e41"></a>`parameters` | `[]` |
- <a id="s-8ad551ca80"></a>Subcommand selection: required.
- <a id="s-67a4aec7a3"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-71dcf44f37"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-eb882e741d"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bfc546042a"></a>`help` | <a id="s-e3d69a941f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-67de891958"></a>`0` | <a id="s-d505e80a6e"></a>`"noncontractual-framework-help"` | <a id="s-b1b248c83b"></a>`"empty"` |

## Governing policies

- <a id="pa-2b48a84c00"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/retrieval/allow_extra_args`
- `/external_contract/cli/piggity/commands/retrieval/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/retrieval/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/retrieval/name`
- `/external_contract/cli/piggity/commands/retrieval/parameters`
- `/external_contract/cli/piggity/commands/retrieval/subcommand_required`
- `/external_contract/cli/piggity/commands/retrieval/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/retrieval/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/retrieval/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/retrieval/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/retrieval/name`

<!-- exact-contract-value: 0d8f15a48e8ad807bb4f2f07e0974112ca07163603e309d3331783f73816a142 -->

```json
"retrieval"
```

### `/external_contract/cli/piggity/commands/retrieval/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/retrieval/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/retrieval/terminating_controls`

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
