# piggity archive

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive:ef30287cc4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-292da0e6c5"></a>Parser name: `archive`

| Field | Value |
|---|---|
| <a id="s-54017c9500"></a>`parameters` | `[]` |
- <a id="s-76d20fc6af"></a>Subcommand selection: required.
- <a id="s-ee8fba0418"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-65ae986227"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-478a36778a"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-88327f2b29"></a>`help` | <a id="s-287a05671b"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-7643408e0a"></a>`0` | <a id="s-c705cd7f2a"></a>`"noncontractual-framework-help"` | <a id="s-2afca29a79"></a>`"empty"` |

## Governing policies

- <a id="pa-1196c30236"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/archive/allow_extra_args`
- `/external_contract/cli/piggity/commands/archive/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/archive/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/archive/name`
- `/external_contract/cli/piggity/commands/archive/parameters`
- `/external_contract/cli/piggity/commands/archive/subcommand_required`
- `/external_contract/cli/piggity/commands/archive/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/archive/name`

<!-- exact-contract-value: d32d6d5eeb28879723113a09a04aac8ec79466cce4e534ee3cf9a29cbf43da24 -->

```json
"archive"
```

### `/external_contract/cli/piggity/commands/archive/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/archive/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/archive/terminating_controls`

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
