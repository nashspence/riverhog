# piggity app key access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-access:4b15aba670 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-271021267b"></a>Parser name: `access`

| Field | Value |
|---|---|
| <a id="s-7a94daaf8f"></a>`parameters` | `[]` |
- <a id="s-4d83ff4285"></a>Subcommand selection: required.
- <a id="s-c9f6a11c6a"></a>Extra arguments at this parser: accepted. Subcommand selection and child parsing still apply.
- <a id="s-60fa4513a7"></a>Options after positional arguments at this parser: left as arguments.
- <a id="s-52c10f0b04"></a>Unknown options at this parser: rejected.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-95a21412d4"></a>`help` | <a id="s-a65b168ec3"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-3a786309b8"></a>`0` | <a id="s-c2db648a9a"></a>`"noncontractual-framework-help"` | <a id="s-7cdfdec114"></a>`"empty"` |

## Governing policies

- <a id="pa-199633f801"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources/authorities.md#src-094022231f) — [reference/riverhog/applications/piggity/src/piggity/main.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/piggity/src/piggity/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/allow_extra_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/allow_interspersed_args`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/ignore_unknown_options`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/parameters`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/subcommand_required`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/access/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/allow_extra_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/allow_interspersed_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/name`

<!-- exact-contract-value: 0fd85c57b5ffac7a9a2a06782aa152dd61b76bf505bb14103d40a3ae1ca4dae2 -->

```json
"access"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/subcommand_required`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/access/terminating_controls`

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
