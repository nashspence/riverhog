# piggity app key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key:213f666fed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-a803109cff"></a>Parser name: `key`

| Field | Value |
|---|---|
| <a id="s-97bf07a8ad"></a>`parameters` | `[]` |
- <a id="s-2448401f41"></a>Subcommand selection: required.
- <a id="s-e217cfcc39"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-e8535a350b"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-c237313e8c"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ef645fe2ba"></a>`help` | <a id="s-7d6cabf1be"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-34c150d24e"></a>`0` | <a id="s-8db225d5ae"></a>`"noncontractual-framework-help"` | <a id="s-3d5bafd31a"></a>`"empty"` |

## Governing policies

- <a id="pa-db0c801ba7"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/allow_extra_args`
- `/external_contract/cli/piggity/commands/app/commands/key/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/app/commands/key/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/app/commands/key/name`
- `/external_contract/cli/piggity/commands/app/commands/key/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/subcommand_required`
- `/external_contract/cli/piggity/commands/app/commands/key/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/name`

<!-- exact-contract-value: c0c1baf2fa65372c83bc5781a31d3ee0bceb37e7917317f33a92825fe5367189 -->

```json
"key"
```

### `/external_contract/cli/piggity/commands/app/commands/key/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/app/commands/key/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/terminating_controls`

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
