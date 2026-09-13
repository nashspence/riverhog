# Explicitly excluded candidates

[Atlas](../index.md) · [Freeze evidence](index.md) · [Policies](../policies/index.md)

This page supports the ‘no more’ side of the audit by naming every discovered release candidate intentionally excluded from freeze protection.

Excluded candidates: **35**

## [exclusion/process-launcher-not-cli/v1](../policies/index.md#p-572523784c)

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

### Excluded candidates

- <a id="x-f7c90a3c1b"></a>`excluded:console-script:riverhog-server:riverhog-api`
  - kind: `console-script`
  - installed target: `riverhog_api.app:main`
- <a id="x-8a9feec556"></a>`excluded:console-script:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_aws.app:main`
- <a id="x-408cee76b2"></a>`excluded:console-script:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_backblaze.app:main`
- <a id="x-327f00ef75"></a>`excluded:console-script:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem`
  - kind: `console-script`
  - installed target: `riverhog_storage_adapter_filesystem.app:main`
- <a id="x-2d9083efa2"></a>`excluded:console-script:stove0-exiftool-observer:stove0-exiftool-observer`
  - kind: `console-script`
  - installed target: `stove0_exiftool_observer.app:main`
- <a id="x-47e82dca49"></a>`excluded:console-script:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer`
  - kind: `console-script`
  - installed target: `stove0_ffprobe_sampling_observer.app:main`
- <a id="x-3d7eda2513"></a>`excluded:console-script:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_review_sampler.app:main`
- <a id="x-f22aa0bb6d"></a>`excluded:console-script:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target`
  - kind: `console-script`
  - installed target: `stove0_nvenc_av1_opus_target.app:target_main`
- <a id="x-b9e0104e17"></a>`excluded:console-script:stove0-opus-review-sampler:stove0-opus-review-sampler`
  - kind: `console-script`
  - installed target: `stove0_opus_review_sampler.app:main`
- <a id="x-d5728703b5"></a>`excluded:console-script:stove0-opus-target:stove0-opus-target`
  - kind: `console-script`
  - installed target: `stove0_opus_target.app:target_main`
- <a id="x-cb02e60cb6"></a>`excluded:console-script:stove0-review-materialize-target:stove0-review-materialize-target`
  - kind: `console-script`
  - installed target: `stove0_review_materialize_target.app:main`
- <a id="x-88e1b73b7a"></a>`excluded:console-script:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target`
  - kind: `console-script`
  - installed target: `stove0_review_rclone_effect_target.app:main`
- <a id="x-27470f9858"></a>`excluded:console-script:stove0-server:stove0-server`
  - kind: `console-script`
  - installed target: `stove0_api.app:main`

### Exact accounting

| Candidate | Boundary source | Detector | Source authority |
|---|---|---|---|
| [excluded:console-script:riverhog-server:riverhog-api](#x-f7c90a3c1b) | `/boundaries/components/70/console_scripts/riverhog-api` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:riverhog-storage-adapter-aws:riverhog-storage-adapter-aws](#x-8a9feec556) | `/boundaries/components/36/console_scripts/riverhog-storage-adapter-aws` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:riverhog-storage-adapter-backblaze:riverhog-storage-adapter-backblaze](#x-408cee76b2) | `/boundaries/components/37/console_scripts/riverhog-storage-adapter-backblaze` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:riverhog-storage-adapter-filesystem:riverhog-storage-adapter-filesystem](#x-327f00ef75) | `/boundaries/components/38/console_scripts/riverhog-storage-adapter-filesystem` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-exiftool-observer:stove0-exiftool-observer](#x-2d9083efa2) | `/boundaries/components/44/console_scripts/stove0-exiftool-observer` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer](#x-47e82dca49) | `/boundaries/components/45/console_scripts/stove0-ffprobe-sampling-observer` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-nvenc-av1-opus-review-sampler:stove0-nvenc-av1-opus-review-sampler](#x-3d7eda2513) | `/boundaries/components/58/console_scripts/stove0-nvenc-av1-opus-review-sampler` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target](#x-f22aa0bb6d) | `/boundaries/components/59/console_scripts/stove0-nvenc-av1-opus-target` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-opus-review-sampler:stove0-opus-review-sampler](#x-b9e0104e17) | `/boundaries/components/60/console_scripts/stove0-opus-review-sampler` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-opus-target:stove0-opus-target](#x-d5728703b5) | `/boundaries/components/61/console_scripts/stove0-opus-target` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-review-materialize-target:stove0-review-materialize-target](#x-cb02e60cb6) | `/boundaries/components/63/console_scripts/stove0-review-materialize-target` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-review-rclone-effect-target:stove0-review-rclone-effect-target](#x-88e1b73b7a) | `/boundaries/components/65/console_scripts/stove0-review-rclone-effect-target` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
| [excluded:console-script:stove0-server:stove0-server](#x-27470f9858) | `/boundaries/components/41/console_scripts/stove0-server` | `release-installed-entry-point` | [release:release.toml](sources.md#src-c5380dbe5f) |
## [exclusion/python-package-no-declared-api/v1](../policies/index.md#p-b061d0042d)

The installed Python package declares no explicit __all__ surface and therefore does not expose a freeze-protected Python API.

### Excluded candidates

- <a id="x-4ea3206c2d"></a>`excluded:python:config-validation:config_validation`
  - kind: `python-package`
  - installed target: `config_validation`
- <a id="x-42f47354bf"></a>`excluded:python:gogurt:gogurt`
  - kind: `python-package`
  - installed target: `gogurt`
- <a id="x-f79b2cbb05"></a>`excluded:python:mango-fish:mango_fish`
  - kind: `python-package`
  - installed target: `mango_fish`
- <a id="x-c6283f060b"></a>`excluded:python:mango-fish:mango_fish.state_migrations`
  - kind: `python-package`
  - installed target: `mango_fish.state_migrations`
- <a id="x-fa4f3a939f"></a>`excluded:python:piggity:piggity`
  - kind: `python-package`
  - installed target: `piggity`
- <a id="x-fb4d8a0933"></a>`excluded:python:piggity:piggity.state_migrations`
  - kind: `python-package`
  - installed target: `piggity.state_migrations`
- <a id="x-aa2e47bc49"></a>`excluded:python:riverhog-provenance-linux-observer:riverhog_provenance_linux_observer`
  - kind: `python-package`
  - installed target: `riverhog_provenance_linux_observer`
- <a id="x-5d50389daf"></a>`excluded:python:riverhog-provenance-macos-observer:riverhog_provenance_macos_observer`
  - kind: `python-package`
  - installed target: `riverhog_provenance_macos_observer`
- <a id="x-ba3f1a52a5"></a>`excluded:python:riverhog-provenance-windows-observer:riverhog_provenance_windows_observer`
  - kind: `python-package`
  - installed target: `riverhog_provenance_windows_observer`
- <a id="x-96d8fa2670"></a>`excluded:python:riverhog-server:riverhog_api`
  - kind: `python-package`
  - installed target: `riverhog_api`
- <a id="x-b1f7a7bf66"></a>`excluded:python:riverhog-server:riverhog_api.routers`
  - kind: `python-package`
  - installed target: `riverhog_api.routers`
- <a id="x-d140bd9eda"></a>`excluded:python:riverhog-server:riverhog_api.schemas`
  - kind: `python-package`
  - installed target: `riverhog_api.schemas`
- <a id="x-ba1fec0722"></a>`excluded:python:riverhog-server:riverhog_core`
  - kind: `python-package`
  - installed target: `riverhog_core`
- <a id="x-17c210ed45"></a>`excluded:python:riverhog-server:riverhog_core.domain`
  - kind: `python-package`
  - installed target: `riverhog_core.domain`
- <a id="x-79d1863dbe"></a>`excluded:python:riverhog-server:riverhog_core.ports`
  - kind: `python-package`
  - installed target: `riverhog_core.ports`
- <a id="x-89bb8a963d"></a>`excluded:python:riverhog-server:riverhog_core.services`
  - kind: `python-package`
  - installed target: `riverhog_core.services`
- <a id="x-5db92e6001"></a>`excluded:python:riverhog-server:riverhog_core.state_migrations`
  - kind: `python-package`
  - installed target: `riverhog_core.state_migrations`
- <a id="x-54aed8ab3f"></a>`excluded:python:riverhog-server:riverhog_core.stores`
  - kind: `python-package`
  - installed target: `riverhog_core.stores`
- <a id="x-65dc4241c3"></a>`excluded:python:riverhog-storage-adapter-backblaze:riverhog_storage_adapter_backblaze`
  - kind: `python-package`
  - installed target: `riverhog_storage_adapter_backblaze`
- <a id="x-835ed3cca7"></a>`excluded:python:stove0-client:stove0_cli`
  - kind: `python-package`
  - installed target: `stove0_cli`
- <a id="x-73780b5109"></a>`excluded:python:stove0-server:stove0_core.state_migrations`
  - kind: `python-package`
  - installed target: `stove0_core.state_migrations`
- <a id="x-3a05c8b322"></a>`excluded:python:time-formats:time_formats`
  - kind: `python-package`
  - installed target: `time_formats`

### Exact accounting

| Candidate | Boundary source | Detector | Source authority |
|---|---|---|---|
| [excluded:python:config-validation:config_validation](#x-4ea3206c2d) | `packages/config-validation/src/config_validation/__init__.py` | `release-wheel-package` | [python:config-validation:config_validation](sources.md#src-47bec59a9e) |
| [excluded:python:gogurt:gogurt](#x-42f47354bf) | `reference/gogurt/application/src/gogurt/__init__.py` | `release-wheel-package` | [python:gogurt:gogurt](sources.md#src-11620caab6) |
| [excluded:python:mango-fish:mango_fish](#x-f79b2cbb05) | `reference/riverhog/applications/mango-fish/src/mango_fish/__init__.py` | `release-wheel-package` | [python:mango-fish:mango_fish](sources.md#src-f1055f88c6) |
| [excluded:python:mango-fish:mango_fish.state_migrations](#x-c6283f060b) | `reference/riverhog/applications/mango-fish/src/mango_fish/state_migrations/__init__.py` | `release-wheel-package` | [python:mango-fish:mango_fish.state_migrations](sources.md#src-96c45baf8e) |
| [excluded:python:piggity:piggity](#x-fa4f3a939f) | `reference/riverhog/applications/piggity/src/piggity/__init__.py` | `release-wheel-package` | [python:piggity:piggity](sources.md#src-c057ec2476) |
| [excluded:python:piggity:piggity.state_migrations](#x-fb4d8a0933) | `reference/riverhog/applications/piggity/src/piggity/state_migrations/__init__.py` | `release-wheel-package` | [python:piggity:piggity.state_migrations](sources.md#src-7d493ce5e0) |
| [excluded:python:riverhog-provenance-linux-observer:riverhog_provenance_linux_observer](#x-aa2e47bc49) | `reference/riverhog/provenance/observers/linux/src/riverhog_provenance_linux_observer/__init__.py` | `release-wheel-package` | [python:riverhog-provenance-linux-observer:riverhog_provenance_linux_observer](sources.md#src-2abaac12f4) |
| [excluded:python:riverhog-provenance-macos-observer:riverhog_provenance_macos_observer](#x-5d50389daf) | `reference/riverhog/provenance/observers/macos/src/riverhog_provenance_macos_observer/__init__.py` | `release-wheel-package` | [python:riverhog-provenance-macos-observer:riverhog_provenance_macos_observer](sources.md#src-2a56adac0b) |
| [excluded:python:riverhog-provenance-windows-observer:riverhog_provenance_windows_observer](#x-ba3f1a52a5) | `reference/riverhog/provenance/observers/windows/src/riverhog_provenance_windows_observer/__init__.py` | `release-wheel-package` | [python:riverhog-provenance-windows-observer:riverhog_provenance_windows_observer](sources.md#src-30471eb7d3) |
| [excluded:python:riverhog-server:riverhog_api](#x-96d8fa2670) | `riverhog/src/riverhog_api/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_api](sources.md#src-067859de12) |
| [excluded:python:riverhog-server:riverhog_api.routers](#x-b1f7a7bf66) | `riverhog/src/riverhog_api/routers/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_api.routers](sources.md#src-30d4a79478) |
| [excluded:python:riverhog-server:riverhog_api.schemas](#x-d140bd9eda) | `riverhog/src/riverhog_api/schemas/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_api.schemas](sources.md#src-61d85b002b) |
| [excluded:python:riverhog-server:riverhog_core](#x-ba1fec0722) | `riverhog/src/riverhog_core/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core](sources.md#src-fcaa6d2479) |
| [excluded:python:riverhog-server:riverhog_core.domain](#x-17c210ed45) | `riverhog/src/riverhog_core/domain/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core.domain](sources.md#src-7ed9f62613) |
| [excluded:python:riverhog-server:riverhog_core.ports](#x-79d1863dbe) | `riverhog/src/riverhog_core/ports/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core.ports](sources.md#src-d1f13c410c) |
| [excluded:python:riverhog-server:riverhog_core.services](#x-89bb8a963d) | `riverhog/src/riverhog_core/services/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core.services](sources.md#src-04f2db8645) |
| [excluded:python:riverhog-server:riverhog_core.state_migrations](#x-5db92e6001) | `riverhog/src/riverhog_core/state_migrations/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core.state_migrations](sources.md#src-ea08488d9c) |
| [excluded:python:riverhog-server:riverhog_core.stores](#x-54aed8ab3f) | `riverhog/src/riverhog_core/stores/__init__.py` | `release-wheel-package` | [python:riverhog-server:riverhog_core.stores](sources.md#src-be0af349bc) |
| [excluded:python:riverhog-storage-adapter-backblaze:riverhog_storage_adapter_backblaze](#x-65dc4241c3) | `reference/riverhog/storage/backblaze/src/riverhog_storage_adapter_backblaze/__init__.py` | `release-wheel-package` | [python:riverhog-storage-adapter-backblaze:riverhog_storage_adapter_backblaze](sources.md#src-18cbb8f253) |
| [excluded:python:stove0-client:stove0_cli](#x-835ed3cca7) | `reference/stove0/application/client/src/stove0_cli/__init__.py` | `release-wheel-package` | [python:stove0-client:stove0_cli](sources.md#src-b2668b2370) |
| [excluded:python:stove0-server:stove0_core.state_migrations](#x-73780b5109) | `reference/stove0/application/server/src/stove0_core/state_migrations/__init__.py` | `release-wheel-package` | [python:stove0-server:stove0_core.state_migrations](sources.md#src-4e7fd480de) |
| [excluded:python:time-formats:time_formats](#x-3a05c8b322) | `packages/time-formats/src/time_formats/__init__.py` | `release-wheel-package` | [python:time-formats:time_formats](sources.md#src-4f4a8234ef) |
