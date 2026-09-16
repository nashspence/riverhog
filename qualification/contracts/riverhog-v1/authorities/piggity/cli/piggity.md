# piggity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity:5effd66027 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-14959e77d6"></a>Parser name: `piggity`
- <a id="s-d42616f68d"></a>Subcommand selection: required.
- <a id="s-6fa5980e03"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-7750bd7b2b"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-c55968e207"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2c4eefdf23"></a>`help` | <a id="s-b1e4586d29"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b211ced558"></a>`0` | <a id="s-bbb0677911"></a>`"noncontractual-framework-help"` | <a id="s-c9ab464264"></a>`"empty"` |
| <a id="s-5beed3d4be"></a>`version` | <a id="s-90f29aa522"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-5c9ead7602"></a>`0` | <a id="s-600fa84ffb"></a>`{"distribution":"piggity","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-cd66052334"></a>`"empty"` |

## Governing policies

- <a id="pa-bb10a51cf9"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/allow_extra_args`
- `/external_contract/cli/piggity/allow_interspersed_args`
- `/external_contract/cli/piggity/ignore_unknown_options`
- `/external_contract/cli/piggity/name`
- `/external_contract/cli/piggity/parameters`
- `/external_contract/cli/piggity/subcommand_required`
- `/external_contract/cli/piggity/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/name`

<!-- exact-contract-value: e19ec9c376ac7f2b93d8f395c47794186488a3a41ca96da52aa308203f6069f6 -->

```json
"piggity"
```

### `/external_contract/cli/piggity/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/terminating_controls`

<!-- exact-contract-value: 338b0ae5501d762e215d995fd6ea1b23d7e7d6f396dded6acc90f5e2c3d6472e -->

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
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "piggity",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```

</details>
