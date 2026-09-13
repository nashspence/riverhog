# V1 contract policies

[Atlas](../index.md)

Policies are defined once here and referenced from every dossier where they are proven to apply. Implementation-correctness witnesses remain outside this contract freeze artifact.

## Compatibility

| Policy | Applications |
|---|---:|
| [compatibility/archive/v1](#p-915b8756ae) | 1 |
| [compatibility/cli/v1](#p-48a89776de) | 168 |
| [compatibility/components/v1](#p-95e9a12259) | 225 |
| [compatibility/configuration/v1](#p-8dc08bb461) | 260 |
| [compatibility/http-api/v1](#p-5bc717c2c0) | 547 |
| [compatibility/python-api/v1](#p-e574772ba5) | 2641 |
| [compatibility/recovery/v1](#p-04aa4508f1) | 1 |

### Definitions

<a id="p-915b8756ae"></a>
#### `compatibility/archive/v1`

A valid v1 archive remains independently recoverable throughout the supported v1 lifetime. Pack construction, ingress checkpointing, and resumable-write strategies are not archive-format compatibility promises.

- Applicability: `["/external_contract/release/compatibility/archive"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **1**
<a id="p-48a89776de"></a>
#### `compatibility/cli/v1`

Command names, options, exit status, and structured output remain backward compatible throughout v1; prose output is for people.

- Applicability: `["/external_contract/release/compatibility/cli"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37d)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2)
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569b)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480b)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d88)
  - [cli:stove0-observer-conformance](../evidence/sources.md#src-5719d140a8)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae2)
  - [cli:stove0-review-planning](../evidence/sources.md#src-ae789ab860)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff4)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c8)
  - [cli:stove0-target-conformance](../evidence/sources.md#src-7a44eec01b)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b5)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **168**
<a id="p-95e9a12259"></a>
#### `compatibility/components/v1`

A supported deployment runs components from one coordinated product version.

- Applicability: `["/external_contract/release/compatibility/components"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa32)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef410)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d23088)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e7549720)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e85)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e832)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a892)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-reparse-point.json](../evidence/sources.md#src-7b71865af2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-security-descriptor.json](../evidence/sources.md#src-1bd0c5feb8)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0ab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad44582)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba1)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)
  - [release-distribution:config-validation](../evidence/sources.md#src-391296b020)
  - [release-distribution:gogurt](../evidence/sources.md#src-3e7b582a54)
  - [release-distribution:gogurt-core](../evidence/sources.md#src-2850fdf46b)
  - [release-distribution:gogurt-linux-listener-host](../evidence/sources.md#src-388e655d12)
  - [release-distribution:gogurt-linux-mounted-volume](../evidence/sources.md#src-db58b362bd)
  - [release-distribution:gogurt-listener-runtime](../evidence/sources.md#src-23725dce0b)
  - [release-distribution:gogurt-macos-listener-host](../evidence/sources.md#src-51ef474638)
  - [release-distribution:gogurt-macos-mounted-volume](../evidence/sources.md#src-ea30a89c8b)
  - [release-distribution:gogurt-path-volume-support](../evidence/sources.md#src-fc01fccb44)
  - [release-distribution:gogurt-windows-listener-host](../evidence/sources.md#src-99f0891144)
  - [release-distribution:gogurt-windows-mounted-volume](../evidence/sources.md#src-d3a2fa67cb)
  - [release-distribution:http-api-contracts](../evidence/sources.md#src-1554c834c7)
  - [release-distribution:lifecycle-events](../evidence/sources.md#src-36336fc3be)
  - [release-distribution:mango-fish](../evidence/sources.md#src-8f9c704431)
  - [release-distribution:piggity](../evidence/sources.md#src-c6f8868ecf)
  - [release-distribution:riverhog-age](../evidence/sources.md#src-2d271f6b70)
  - [release-distribution:riverhog-application-access](../evidence/sources.md#src-1524c1360d)
  - [release-distribution:riverhog-archive-contracts](../evidence/sources.md#src-f6ac304b52)
  - [release-distribution:riverhog-client](../evidence/sources.md#src-7d25f2c873)
  - [release-distribution:riverhog-ftp-adapter](../evidence/sources.md#src-37bda529c9)
  - [release-distribution:riverhog-ftp-adapter-api-client](../evidence/sources.md#src-70a41aeb48)
  - [release-distribution:riverhog-protocol](../evidence/sources.md#src-1221d32c9e)
  - [release-distribution:riverhog-provenance](../evidence/sources.md#src-95dbd50af1)
  - [release-distribution:riverhog-provenance-contracts](../evidence/sources.md#src-d222cdb3b8)
  - [release-distribution:riverhog-provenance-linux-contracts](../evidence/sources.md#src-0bc97c8819)
  - [release-distribution:riverhog-provenance-linux-observer](../evidence/sources.md#src-511bdc989a)
  - [release-distribution:riverhog-provenance-macos-contracts](../evidence/sources.md#src-7b7622c15a)
  - [release-distribution:riverhog-provenance-macos-observer](../evidence/sources.md#src-40294474bf)
  - [release-distribution:riverhog-provenance-windows-contracts](../evidence/sources.md#src-3899e85b99)
  - [release-distribution:riverhog-provenance-windows-observer](../evidence/sources.md#src-39aea5b4b6)
  - [release-distribution:riverhog-recover](../evidence/sources.md#src-917183ebd1)
  - [release-distribution:riverhog-server](../evidence/sources.md#src-7debc5c818)
  - [release-distribution:riverhog-storage-adapter-asgi-support](../evidence/sources.md#src-f4e68c2bf9)
  - [release-distribution:riverhog-storage-adapter-aws](../evidence/sources.md#src-ef11c798d9)
  - [release-distribution:riverhog-storage-adapter-backblaze](../evidence/sources.md#src-039f9af430)
  - [release-distribution:riverhog-storage-adapter-filesystem](../evidence/sources.md#src-58a272031a)
  - [release-distribution:riverhog-storage-adapter-protocol](../evidence/sources.md#src-986408de09)
  - [release-distribution:riverhog-storage-adapter-s3-support](../evidence/sources.md#src-19239f8eca)
  - [release-distribution:riverhog-storage-adapter-support](../evidence/sources.md#src-e3b24ac45f)
  - [release-distribution:state-schema](../evidence/sources.md#src-07745187f2)
  - [release-distribution:stove0-api-client](../evidence/sources.md#src-65fbc03822)
  - [release-distribution:stove0-client](../evidence/sources.md#src-2b4e27da80)
  - [release-distribution:stove0-exiftool-observer](../evidence/sources.md#src-8b7b7e5eed)
  - [release-distribution:stove0-ffprobe-sampling-observer](../evidence/sources.md#src-e01a1e596d)
  - [release-distribution:stove0-media-archive-target-contracts](../evidence/sources.md#src-7f1e7fec9e)
  - [release-distribution:stove0-media-archive-target-support](../evidence/sources.md#src-0d39629f02)
  - [release-distribution:stove0-media-metadata-observer-contracts](../evidence/sources.md#src-64ab741526)
  - [release-distribution:stove0-media-sampling-observer-contracts](../evidence/sources.md#src-135c11f96b)
  - [release-distribution:stove0-nvenc-av1-opus-review-sampler](../evidence/sources.md#src-27f096c998)
  - [release-distribution:stove0-nvenc-av1-opus-target](../evidence/sources.md#src-b520cbce2a)
  - [release-distribution:stove0-observer-client](../evidence/sources.md#src-ada50e7589)
  - [release-distribution:stove0-observer-protocol](../evidence/sources.md#src-7c927fed76)
  - [release-distribution:stove0-observer-support](../evidence/sources.md#src-49de4b2b28)
  - [release-distribution:stove0-operator-contracts](../evidence/sources.md#src-3697a2cd79)
  - [release-distribution:stove0-opus-review-sampler](../evidence/sources.md#src-21ddb4cd93)
  - [release-distribution:stove0-opus-target](../evidence/sources.md#src-710aa0c3de)
  - [release-distribution:stove0-protocol](../evidence/sources.md#src-8793f1ad67)
  - [release-distribution:stove0-recipe-config](../evidence/sources.md#src-124da7769c)
  - [release-distribution:stove0-review-materialize-target](../evidence/sources.md#src-8b1c7b0fd5)
  - [release-distribution:stove0-review-planning](../evidence/sources.md#src-41edb4d9f8)
  - [release-distribution:stove0-review-rclone-effect-target](../evidence/sources.md#src-e1595567d0)
  - [release-distribution:stove0-review-sampler-client](../evidence/sources.md#src-9090dabce1)
  - [release-distribution:stove0-review-sampler-protocol](../evidence/sources.md#src-248793bf47)
  - [release-distribution:stove0-review-sampler-support](../evidence/sources.md#src-08f9b590a6)
  - [release-distribution:stove0-review-target-contracts](../evidence/sources.md#src-288a68ea62)
  - [release-distribution:stove0-review-target-support](../evidence/sources.md#src-56e42e89a6)
  - [release-distribution:stove0-server](../evidence/sources.md#src-56a02fc153)
  - [release-distribution:stove0-target-client](../evidence/sources.md#src-370da30421)
  - [release-distribution:stove0-target-protocol](../evidence/sources.md#src-182457b760)
  - [release-distribution:stove0-target-support](../evidence/sources.md#src-745a95dbc1)
  - [release-distribution:time-formats](../evidence/sources.md#src-514b283d5f)
  - [release-images:docker-bake](../evidence/sources.md#src-8d3f4df21c)
  - [release-installation:planner](../evidence/sources.md#src-d1a927fc4b)
  - [release-publication:planner](../evidence/sources.md#src-03a2f48338)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)
  - [state:gogurt-listener](../evidence/sources.md#src-6b3ecfced3)
  - [state:mango-fish-cursor](../evidence/sources.md#src-b1cc215b8d)
  - [state:piggity-local](../evidence/sources.md#src-f6a1289f67)
  - [state:riverhog-catalog](../evidence/sources.md#src-d8b4a14670)
  - [state:riverhog-ftp-custody](../evidence/sources.md#src-54f88a3a47)
  - [state:riverhog-provenance-installation](../evidence/sources.md#src-080b970190)
  - [state:stove0-control](../evidence/sources.md#src-45e44b17fd)
  - [state:stove0-target-jobs](../evidence/sources.md#src-7b4138829a)

Applications: **225**
<a id="p-8dc08bb461"></a>
#### `compatibility/configuration/v1`

Accepted v1 configuration remains valid throughout v1 unless an unsafe value must be rejected.

- Applicability: `["/external_contract/release/compatibility/configuration"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../evidence/sources.md#src-3071ba44b3)
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](../evidence/sources.md#src-956dbca0d9)
  - [configuration-environment:gogurt-linux-listener-host:XDG_CONFIG_HOME](../evidence/sources.md#src-82ea2d68db)
  - [configuration-environment:gogurt-linux-listener-host:XDG_STATE_HOME](../evidence/sources.md#src-7517c8a6b8)
  - [configuration-environment:gogurt-windows-listener-host:LOCALAPPDATA](../evidence/sources.md#src-25da23d25d)
  - [configuration-environment:gogurt-windows-listener-host:PATHEXT](../evidence/sources.md#src-3463aeca7f)
  - [configuration-environment:gogurt-windows-listener-host:SystemRoot](../evidence/sources.md#src-1770457f4b)
  - [configuration-environment:piggity:PIGGITY_LOCAL_DATABASE](../evidence/sources.md#src-ab1d0074de)
  - [configuration-environment:piggity:PIGGITY_LOCAL_ROOT](../evidence/sources.md#src-7c4314c2cb)
  - [configuration-environment:piggity:PIGGITY_PLAIN](../evidence/sources.md#src-88f78b70a7)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-283d9ea8ad)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2057e6d1cc)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-40c863880d)
  - [configuration-environment:piggity:TERM](../evidence/sources.md#src-6ee2b49508)
  - [configuration-environment:riverhog-client:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-6501ce08a3)
  - [configuration-environment:riverhog-client:RIVERHOG_BASE_URL](../evidence/sources.md#src-475671fe74)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-bb20193981)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-4629108138)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-7d03d4b83f)
  - [configuration-environment:riverhog-client:RIVERHOG_HOST_HEADER](../evidence/sources.md#src-9a32401dbf)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP2](../evidence/sources.md#src-1774dabb11)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-908bba2b9b)
  - [configuration-environment:riverhog-client:RIVERHOG_TOKEN](../evidence/sources.md#src-cc55a54933)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-5d0adeaf5d)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-93e4082935)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-6e31cc159d)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-85f3c43f72)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_BASE_URL](../evidence/sources.md#src-aa9adbb30a)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP2](../evidence/sources.md#src-dd720ed781)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-97bfcc2b77)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_TOKEN](../evidence/sources.md#src-5a52b017b3)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-8be0f96180)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_BASE_URL](../evidence/sources.md#src-b93808cfe5)
  - [configuration-environment:riverhog-ftp-adapter:RIVERHOG_FTP_ADAPTER_CONFIG](../evidence/sources.md#src-67c239e18e)
  - [configuration-environment:riverhog-provenance:LOCALAPPDATA](../evidence/sources.md#src-d5c0e21c89)
  - [configuration-environment:riverhog-provenance:RIVERHOG_PROVENANCE_STATE_HOME](../evidence/sources.md#src-9fe04e9d42)
  - [configuration-environment:riverhog-provenance:XDG_STATE_HOME](../evidence/sources.md#src-fb8d65585b)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-d1018a4c53)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-b3f6d318c3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_ACTIVE_PASSPHRASE_ID](../evidence/sources.md#src-59ce1b7699)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-309bc75357)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PASSPHRASES_JSON](../evidence/sources.md#src-44d728aefc)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-36a45a4230)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_READ_ORDER](../evidence/sources.md#src-b743528fa3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_SCRYPT_WORK_FACTOR](../evidence/sources.md#src-3434cbb4f1)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_STORES](../evidence/sources.md#src-b4408185b7)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-b5ab0e168b)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-19a319fc4e)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-66ed403dd1)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_STORE](../evidence/sources.md#src-2a3a35733e)
  - [configuration-environment:riverhog-server:RIVERHOG_BOOTSTRAP_TOKEN](../evidence/sources.md#src-46bdbba342)
  - [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_LIFETIME](../evidence/sources.md#src-9cc099a19b)
  - [configuration-environment:riverhog-server:RIVERHOG_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-70d116fdbc)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_BOOTSTRAP_LIFETIME](../evidence/sources.md#src-bb4e9ed43a)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_CURSOR_LIFETIME](../evidence/sources.md#src-afa71403d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-e04d565c18)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-735831a8d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-65433124fb)
  - [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-4cfc3a8358)
  - [configuration-environment:riverhog-server:RIVERHOG_DATABASE_URL](../evidence/sources.md#src-c84b874748)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-fef401b6a9)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-42756a2b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_SOURCE](../evidence/sources.md#src-cc7e1cd9d7)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-e1d4295958)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-bf9ecd1b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_LOG_LEVEL](../evidence/sources.md#src-cb4528636e)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_FILES](../evidence/sources.md#src-7d59fe1eef)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-f8aff5063d)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-b2ca118143)
  - [configuration-environment:riverhog-server:RIVERHOG_PUBLIC_BASE_URL](../evidence/sources.md#src-fbbd173a78)
  - [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-3fb717c8c3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_ENABLED](../evidence/sources.md#src-257c42be6d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-22c1f9d04c)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_STORES](../evidence/sources.md#src-51dd06072e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-d858623918)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-26efc77b40)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-4fd9a92a9d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_ESTIMATED_LATENCY](../evidence/sources.md#src-63c6f042b8)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-cb29acf59b)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c44aeb9ce3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-1e662f4cb2)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-01bd40bbb3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_BILLING_MODE](../evidence/sources.md#src-d46028cd7f)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-972a8351e5)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-236f08fcd9)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-a586c2cf3e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-4c5aea570f)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID](../evidence/sources.md#src-79fd55b300)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE](../evidence/sources.md#src-9502ea113d)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ARCHIVE_STORAGE_CLASS](../evidence/sources.md#src-c7af32e338)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_BUCKET](../evidence/sources.md#src-363b39cd6e)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_BASE_URL](../evidence/sources.md#src-244cb79acb)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PRIVATE_KEY_PATH](../evidence/sources.md#src-4c2532ee7e)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CLOUDFRONT_PUBLIC_KEY_ID](../evidence/sources.md#src-1bbc37d425)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../evidence/sources.md#src-be41c0e7eb)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ENDPOINT_URL](../evidence/sources.md#src-88014cadc1)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_FORCE_PATH_STYLE](../evidence/sources.md#src-64b0aa7cef)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_HOST](../evidence/sources.md#src-bb7f6a2fb9)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_IMMEDIATE_STORAGE_CLASS](../evidence/sources.md#src-1f7b750083)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_ATTEMPTS](../evidence/sources.md#src-989751bd37)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../evidence/sources.md#src-0419b135c2)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_PORT](../evidence/sources.md#src-75dcbe865e)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-f9cdca0f7d)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_MODE](../evidence/sources.md#src-45d661871e)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../evidence/sources.md#src-47cfeed976)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_REGION](../evidence/sources.md#src-68cd9cdbbc)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_DAYS](../evidence/sources.md#src-d42f940c08)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_RESTORE_TIER](../evidence/sources.md#src-15442d9633)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_RETRY_MODE](../evidence/sources.md#src-8b8b0e39d8)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_ROOT_PREFIX](../evidence/sources.md#src-4960800462)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY](../evidence/sources.md#src-6b5c5953fa)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE](../evidence/sources.md#src-9390bb5416)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN](../evidence/sources.md#src-74ac0a4c8b)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_SESSION_TOKEN_FILE](../evidence/sources.md#src-e8515dc45f)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TCP_KEEPALIVE](../evidence/sources.md#src-149ae68f5d)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN](../evidence/sources.md#src-4a894c7712)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_TOKEN_FILE](../evidence/sources.md#src-86a691ce39)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID](../evidence/sources.md#src-4f13526f1c)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ACCESS_KEY_ID_FILE](../evidence/sources.md#src-9b1dab5eef)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_BUCKET](../evidence/sources.md#src-462fbc2e65)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../evidence/sources.md#src-bb79933c34)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ENDPOINT_URL](../evidence/sources.md#src-4667488734)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_FORCE_PATH_STYLE](../evidence/sources.md#src-5ebbe5a454)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_HOST](../evidence/sources.md#src-86e7da0f27)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS](../evidence/sources.md#src-2072a40ae0)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../evidence/sources.md#src-eba038dc56)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_PORT](../evidence/sources.md#src-4d7ca8fcb5)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-8a38b0e993)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../evidence/sources.md#src-7e10a8a388)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_REGION](../evidence/sources.md#src-7866c27522)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_RETRY_MODE](../evidence/sources.md#src-eb973fcaa3)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_ROOT_PREFIX](../evidence/sources.md#src-d359d34ec3)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY](../evidence/sources.md#src-bd0797864c)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_SECRET_ACCESS_KEY_FILE](../evidence/sources.md#src-3081b5b193)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TCP_KEEPALIVE](../evidence/sources.md#src-ca135410c6)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN](../evidence/sources.md#src-e5a43c6259)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_TOKEN_FILE](../evidence/sources.md#src-487ca76c79)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_HOST](../evidence/sources.md#src-7bb822dcfc)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](../evidence/sources.md#src-3a1b9a87f7)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_PORT](../evidence/sources.md#src-68937d0a09)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-7b65a90cf1)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_ROOT](../evidence/sources.md#src-fb670e5991)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES](../evidence/sources.md#src-d0691ed2c9)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN](../evidence/sources.md#src-ef3b5a31c7)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_TOKEN_FILE](../evidence/sources.md#src-db67f94747)
  - [configuration-environment:stove0-api-client:STOVE0_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-a9f40fdb75)
  - [configuration-environment:stove0-api-client:STOVE0_BASE_URL](../evidence/sources.md#src-34ec96f5f9)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP2](../evidence/sources.md#src-29a943f529)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-a470f49fc7)
  - [configuration-environment:stove0-api-client:STOVE0_TOKEN](../evidence/sources.md#src-c052f8f5ca)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_BIN](../evidence/sources.md#src-db3b5f3ab1)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_HOST](../evidence/sources.md#src-8dfcd7e460)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-e2f881a54b)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_PORT](../evidence/sources.md#src-ee07e99c6c)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-29eef6aa4c)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN](../evidence/sources.md#src-5cf4e00bd2)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-7e8e1ce139)
  - [configuration-environment:stove0-exiftool-observer:STOVE0_EXIFTOOL_OBSERVER_WORKSPACE](../evidence/sources.md#src-98428ceea4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_BIN](../evidence/sources.md#src-2e8c92027a)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_HOST](../evidence/sources.md#src-1ede0d200b)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_IMAGE_DIGEST](../evidence/sources.md#src-64a7dc58ec)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_PORT](../evidence/sources.md#src-b6e1a506b4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_SOURCE_REVISION](../evidence/sources.md#src-776ca532af)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN](../evidence/sources.md#src-42afc1d184)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN_FILE](../evidence/sources.md#src-476999fdb4)
  - [configuration-environment:stove0-ffprobe-sampling-observer:STOVE0_FFPROBE_SAMPLING_OBSERVER_WORKSPACE](../evidence/sources.md#src-c36f19fb69)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-f77354177a)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_HOST](../evidence/sources.md#src-bfa433528a)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST](../evidence/sources.md#src-669569f115)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_PORT](../evidence/sources.md#src-aab9b1006d)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_SOURCE_REVISION](../evidence/sources.md#src-8dd2815ac9)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN](../evidence/sources.md#src-c3e5740197)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_TOKEN_FILE](../evidence/sources.md#src-0c482b4895)
  - [configuration-environment:stove0-nvenc-av1-opus-review-sampler:STOVE0_NVENC_AV1_OPUS_REVIEW_SAMPLER_WORKSPACE](../evidence/sources.md#src-6b93ebbc94)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-648abac914)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_HOST](../evidence/sources.md#src-3754100ddf)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST](../evidence/sources.md#src-fd772770ec)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_PORT](../evidence/sources.md#src-fb16b17c23)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_SOURCE_REVISION](../evidence/sources.md#src-5e5b556d66)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_STATE_ROOT](../evidence/sources.md#src-d24f94160a)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN](../evidence/sources.md#src-1b1ad33ef7)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_TOKEN_FILE](../evidence/sources.md#src-a8872fc126)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_WORKSPACE](../evidence/sources.md#src-08eeb13134)
  - [configuration-environment:stove0-nvenc-av1-opus-target:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD](../evidence/sources.md#src-6bc691e964)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-194d028459)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_HOST](../evidence/sources.md#src-7e5f631953)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_IMAGE_DIGEST](../evidence/sources.md#src-7dcd2e53f5)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_PORT](../evidence/sources.md#src-c80bc2cb9e)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION](../evidence/sources.md#src-91897194ea)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN](../evidence/sources.md#src-3e7ab7516f)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_TOKEN_FILE](../evidence/sources.md#src-009b1d7ca5)
  - [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_WORKSPACE](../evidence/sources.md#src-86f3eedb7a)
  - [configuration-environment:stove0-opus-target:STOVE0_FFMPEG_BIN](../evidence/sources.md#src-854b16672a)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_HOST](../evidence/sources.md#src-8aa1fc04d2)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_IMAGE_DIGEST](../evidence/sources.md#src-284da96ce7)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_PORT](../evidence/sources.md#src-9e592324a6)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_SOURCE_REVISION](../evidence/sources.md#src-2bdb4a9e8f)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_STATE_ROOT](../evidence/sources.md#src-ba7aa199b0)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_TOKEN](../evidence/sources.md#src-4e4d5f3e40)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_TOKEN_FILE](../evidence/sources.md#src-0d6bd47b40)
  - [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_WORKSPACE](../evidence/sources.md#src-d621aac3e2)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_HOST](../evidence/sources.md#src-d1bde5c693)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_IMAGE_DIGEST](../evidence/sources.md#src-13191bb470)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_PORT](../evidence/sources.md#src-bafe419002)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON](../evidence/sources.md#src-b822de9d0e)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SAMPLERS_JSON_FILE](../evidence/sources.md#src-0f35eb76bc)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_SOURCE_REVISION](../evidence/sources.md#src-3727f03fe5)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_STATE_ROOT](../evidence/sources.md#src-e6c90cc875)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN](../evidence/sources.md#src-d8db24208d)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_TOKEN_FILE](../evidence/sources.md#src-569110a206)
  - [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_WORKSPACE](../evidence/sources.md#src-c812dca17e)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_DESTINATION_IDENTITY](../evidence/sources.md#src-13f168e9ef)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_HOST](../evidence/sources.md#src-eec7e56995)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_IMAGE_DIGEST](../evidence/sources.md#src-b9a8b82806)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_PORT](../evidence/sources.md#src-333e7531f6)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_BIN](../evidence/sources.md#src-8731ef7028)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_CONFIG_FILE](../evidence/sources.md#src-ff3d8661eb)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_REMOTE](../evidence/sources.md#src-c6d8df82f9)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS](../evidence/sources.md#src-7f004c5323)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON](../evidence/sources.md#src-442d08ddb7)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SAMPLERS_JSON_FILE](../evidence/sources.md#src-f24df93f95)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_SOURCE_REVISION](../evidence/sources.md#src-119536f200)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_STATE_ROOT](../evidence/sources.md#src-34de43ae77)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN](../evidence/sources.md#src-f997a24e0d)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_TOKEN_FILE](../evidence/sources.md#src-58167f74c0)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_WORKSPACE](../evidence/sources.md#src-a92b6498b5)
  - [configuration-environment:stove0-server:RIVERHOG_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-ab009ffd92)
  - [configuration-environment:stove0-server:RIVERHOG_BASE_URL](../evidence/sources.md#src-94aba7e378)
  - [configuration-environment:stove0-server:RIVERHOG_TOKEN](../evidence/sources.md#src-a2d6243ee8)
  - [configuration-environment:stove0-server:RIVERHOG_TOKEN_FILE](../evidence/sources.md#src-03d80abe0b)
  - [configuration-environment:stove0-server:STOVE0_ADMISSIONS_PATH](../evidence/sources.md#src-3e52273877)
  - [configuration-environment:stove0-server:STOVE0_API_TOKEN](../evidence/sources.md#src-b54731a17b)
  - [configuration-environment:stove0-server:STOVE0_API_TOKEN_FILE](../evidence/sources.md#src-5b16a09dac)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-8e79bc10a5)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY](../evidence/sources.md#src-f552366814)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_SIGNING_KEY_FILE](../evidence/sources.md#src-4c41ceb811)
  - [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-0e9a09a14d)
  - [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-1997f656ba)
  - [configuration-environment:stove0-server:STOVE0_DATABASE_URL](../evidence/sources.md#src-d05255fc63)
  - [configuration-environment:stove0-server:STOVE0_DATABASE_URL_FILE](../evidence/sources.md#src-6a24b678de)
  - [configuration-environment:stove0-server:STOVE0_OBSERVERS_JSON](../evidence/sources.md#src-5e770d1e0f)
  - [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-024a6340ee)
  - [configuration-environment:stove0-server:STOVE0_RECIPES_PATH](../evidence/sources.md#src-afbf7c2b57)
  - [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1ba44b59b5)
  - [configuration-environment:stove0-server:STOVE0_TARGETS_JSON](../evidence/sources.md#src-05b1eb365e)
  - [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-86bb163042)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../evidence/sources.md#src-00eaae2681)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_BASE_URL](../evidence/sources.md#src-074108cdde)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY](../evidence/sources.md#src-543ac98fff)
  - [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE](../evidence/sources.md#src-67af4f66b9)
  - [configuration-environment:stove0-server:STOVE0_WORKSPACE_ASSURANCE](../evidence/sources.md#src-26ac73d782)
  - [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-a4a7b5aed4)
  - [configuration:gogurt-core:configuration:gogurt-routes-schema](../evidence/sources.md#src-75f9616966)
  - [configuration:mango-fish:configuration:mango-fish-config](../evidence/sources.md#src-fead015e98)
  - [configuration:riverhog-ftp-adapter:configuration:ftp-adapter-config](../evidence/sources.md#src-cf8c44826f)
  - [configuration:riverhog-ftp-adapter:configuration:source-config](../evidence/sources.md#src-6674ffa9af)
  - [configuration:stove0-operator-contracts:configuration:admission-catalog](../evidence/sources.md#src-c0d7e75302)
  - [configuration:stove0-recipe-config:configuration:recipe-catalog](../evidence/sources.md#src-28686050a7)
  - [configuration:stove0-review-target-support:configuration:review-target-config](../evidence/sources.md#src-cca9387ce6)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **260**
<a id="p-5bc717c2c0"></a>
#### `compatibility/http-api/v1`

Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1.

- Applicability: `["/external_contract/release/compatibility/http_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **547**
<a id="p-e574772ba5"></a>
#### `compatibility/python-api/v1`

Freeze-protected declared public-module exports and public signatures remain backward compatible throughout v1; importable packages explicitly excluded from the Python surface are not Python API promises.

- Applicability: `["/external_contract/release/compatibility/python_api"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [python:gogurt-core:gogurt_core](../evidence/sources.md#src-e253e4a684)
  - [python:gogurt-linux-listener-host:gogurt_linux_listener_host](../evidence/sources.md#src-78f263d456)
  - [python:gogurt-linux-mounted-volume:gogurt_linux_mounted_volume](../evidence/sources.md#src-dfbc0b0c2f)
  - [python:gogurt-listener-runtime:gogurt_listener_runtime](../evidence/sources.md#src-259980dd25)
  - [python:gogurt-macos-listener-host:gogurt_macos_listener_host](../evidence/sources.md#src-3a09f7fc10)
  - [python:gogurt-macos-mounted-volume:gogurt_macos_mounted_volume](../evidence/sources.md#src-d5c41c5cfc)
  - [python:gogurt-path-volume-support:gogurt_path_volume_support](../evidence/sources.md#src-a67d948855)
  - [python:gogurt-windows-listener-host:gogurt_windows_listener_host](../evidence/sources.md#src-ec25d3db2b)
  - [python:gogurt-windows-mounted-volume:gogurt_windows_mounted_volume](../evidence/sources.md#src-3af4750524)
  - [python:http-api-contracts:http_api_contracts](../evidence/sources.md#src-a522df4cfd)
  - [python:lifecycle-events:lifecycle_events](../evidence/sources.md#src-7ab82f5e27)
  - [python:riverhog-age:riverhog_age](../evidence/sources.md#src-a842e50b8b)
  - [python:riverhog-application-access:riverhog_application_access](../evidence/sources.md#src-9d9ce5fdac)
  - [python:riverhog-archive-contracts:riverhog_archive_contracts](../evidence/sources.md#src-4557222ddc)
  - [python:riverhog-client:riverhog_client](../evidence/sources.md#src-c149020c71)
  - [python:riverhog-client:riverhog_client.transform](../evidence/sources.md#src-7a247bb534)
  - [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../evidence/sources.md#src-a83ae875ae)
  - [python:riverhog-ftp-adapter:riverhog_ftp_adapter](../evidence/sources.md#src-8d11f8fa97)
  - [python:riverhog-protocol:riverhog_protocol](../evidence/sources.md#src-19e35f15d9)
  - [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../evidence/sources.md#src-9b6289a988)
  - [python:riverhog-provenance-linux-contracts:riverhog_provenance_linux_contracts](../evidence/sources.md#src-2cb2292124)
  - [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../evidence/sources.md#src-75fa891e7c)
  - [python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts](../evidence/sources.md#src-0d5e61922a)
  - [python:riverhog-provenance:riverhog_provenance](../evidence/sources.md#src-38ef3a6054)
  - [python:riverhog-recover:riverhog_recover](../evidence/sources.md#src-dbfe6c5e2e)
  - [python:riverhog-storage-adapter-asgi-support:riverhog_storage_adapter_asgi_support](../evidence/sources.md#src-faaefe65d4)
  - [python:riverhog-storage-adapter-aws:riverhog_storage_adapter_aws](../evidence/sources.md#src-5059355196)
  - [python:riverhog-storage-adapter-filesystem:riverhog_storage_adapter_filesystem](../evidence/sources.md#src-e075952170)
  - [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../evidence/sources.md#src-2da8857a83)
  - [python:riverhog-storage-adapter-s3-support:riverhog_storage_adapter_s3_support](../evidence/sources.md#src-aa14de5031)
  - [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../evidence/sources.md#src-284271cd54)
  - [python:state-schema:state_schema](../evidence/sources.md#src-57d87d192c)
  - [python:stove0-api-client:stove0_api_client](../evidence/sources.md#src-5d52ac5998)
  - [python:stove0-exiftool-observer:stove0_exiftool_observer](../evidence/sources.md#src-18b5d27762)
  - [python:stove0-ffprobe-sampling-observer:stove0_ffprobe_sampling_observer](../evidence/sources.md#src-7974c3bc25)
  - [python:stove0-media-archive-target-contracts:stove0_media_archive_target_contracts](../evidence/sources.md#src-dfeb5229f2)
  - [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../evidence/sources.md#src-3caa343b1d)
  - [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../evidence/sources.md#src-8d1649a0f1)
  - [python:stove0-media-sampling-observer-contracts:stove0_media_sampling_observer_contracts](../evidence/sources.md#src-b9344d06d7)
  - [python:stove0-nvenc-av1-opus-review-sampler:stove0_nvenc_av1_opus_review_sampler](../evidence/sources.md#src-dc2d09f8b3)
  - [python:stove0-nvenc-av1-opus-target:stove0_nvenc_av1_opus_target](../evidence/sources.md#src-b6e7b93ef1)
  - [python:stove0-observer-client:stove0_observer_client](../evidence/sources.md#src-67dbe161ba)
  - [python:stove0-observer-protocol:stove0_observer_protocol](../evidence/sources.md#src-62450e0156)
  - [python:stove0-observer-support:stove0_observer_support](../evidence/sources.md#src-13bf3acd32)
  - [python:stove0-operator-contracts:stove0_operator_contracts](../evidence/sources.md#src-51ad84528d)
  - [python:stove0-opus-review-sampler:stove0_opus_review_sampler](../evidence/sources.md#src-8c27cdb902)
  - [python:stove0-opus-target:stove0_opus_target](../evidence/sources.md#src-9164f15983)
  - [python:stove0-protocol:stove0_protocol](../evidence/sources.md#src-084138045e)
  - [python:stove0-recipe-config:stove0_recipe_config](../evidence/sources.md#src-9e1422d2d6)
  - [python:stove0-review-materialize-target:stove0_review_materialize_target](../evidence/sources.md#src-d3426939d3)
  - [python:stove0-review-planning:stove0_review_planning](../evidence/sources.md#src-354ae519e9)
  - [python:stove0-review-rclone-effect-target:stove0_review_rclone_effect_target](../evidence/sources.md#src-5fd1cb5bbe)
  - [python:stove0-review-sampler-client:stove0_review_sampler_client](../evidence/sources.md#src-4a777c675f)
  - [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../evidence/sources.md#src-25c43eb779)
  - [python:stove0-review-sampler-support:stove0_review_sampler_support](../evidence/sources.md#src-6dd798b0df)
  - [python:stove0-review-target-contracts:stove0_review_target_contracts](../evidence/sources.md#src-1d0886e380)
  - [python:stove0-review-target-support:stove0_review_target_support](../evidence/sources.md#src-2a89a71c41)
  - [python:stove0-server:stove0_api](../evidence/sources.md#src-d5a12e8c56)
  - [python:stove0-server:stove0_core](../evidence/sources.md#src-7558b08e7f)
  - [python:stove0-target-client:stove0_target_client](../evidence/sources.md#src-be4c80156f)
  - [python:stove0-target-protocol:stove0_target_protocol](../evidence/sources.md#src-f4f0b22026)
  - [python:stove0-target-support:stove0_target_support](../evidence/sources.md#src-3c01163237)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **2641**
<a id="p-04aa4508f1"></a>
#### `compatibility/recovery/v1`

Every later v1 recovery release reads every valid earlier v1 archive and provenance set.

- Applicability: `["/external_contract/release/compatibility/recovery"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **1**

## Extent Principles

| Policy | Applications |
|---|---:|
| [extent-principle/bounded-work/v1](#p-a16724dfa0) | 1 |
| [extent-principle/configuration/v1](#p-aa533611e9) | 1 |
| [extent-principle/implementation-privacy/v1](#p-5f9f32ebd9) | 1 |
| [extent-principle/logical-totals/v1](#p-cfe2e12ee6) | 1 |
| [extent-principle/operational-capacity/v1](#p-fac1e46f10) | 1 |

### Definitions

<a id="p-a16724dfa0"></a>
#### `extent-principle/bounded-work/v1`

Large logical totals cross bounded pages, segments, or restartable work steps; a carrier bound does not redefine the logical total.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-aa533611e9"></a>
#### `extent-principle/configuration/v1`

Hardware- or environment-dependent limits that observably affect accepted work are operator-configurable and source-linked.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-5f9f32ebd9"></a>
#### `extent-principle/implementation-privacy/v1`

Buffers, provider mechanics, database layout, and other non-observable implementation extents are not frozen here.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-cfe2e12ee6"></a>
#### `extent-principle/logical-totals/v1`

A finite logical total has no product-level semantic maximum unless its owning contract declares one.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**
<a id="p-fac1e46f10"></a>
#### `extent-principle/operational-capacity/v1`

Capacity policy may explicitly reject, defer, or throttle work, but must not silently truncate it or become an undocumented semantic ceiling.

- Applicability: `["/external_contract/extents"]`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **1**

## Extent Rules

| Policy | Applications |
|---|---:|
| [extent-rule/bounded-segment/v1](#p-2b3f3f1594) | 15 |
| [extent-rule/configuration-composition/v1](#p-dcd344e8e5) | 8 |
| [extent-rule/configured-capacity/v1](#p-3ebc61fc99) | 66 |
| [extent-rule/extension-contract/v1](#p-75a89f9d1c) | 17 |
| [extent-rule/no-semantic-maximum/v1](#p-574724b48a) | 166 |
| [extent-rule/route-progression/v1](#p-6b76b527cb) | 67 |
| [extent-rule/schema-bound/v1](#p-c0db822fc0) | 454 |

### Definitions

<a id="p-2b3f3f1594"></a>
#### `extent-rule/bounded-segment/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's x-riverhog-extent declaration |
| `completion` | the owner-declared progression or repeated-work contract |
| `exceeded` | bounded-carrier-validation-error |
| `policy` | segmented_no_total_max |
| `semantic_maximum` | None |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owner-declared progression or repeated-work contract", "exceeded": "bounded-carrier-validation-error"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)

Applications: **15**
<a id="p-dcd344e8e5"></a>
#### `extent-rule/configuration-composition/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning validated deployment configuration document |
| `declared_operational_maximum` | None |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"hidden_maximum": "forbidden", "silent_truncation": "forbidden"}`
- Executable authorities:
  - [configuration:gogurt-core:configuration:gogurt-routes-schema](../evidence/sources.md#src-75f9616966)
  - [configuration:mango-fish:configuration:mango-fish-config](../evidence/sources.md#src-fead015e98)
  - [configuration:riverhog-ftp-adapter:configuration:ftp-adapter-config](../evidence/sources.md#src-cf8c44826f)
  - [configuration:riverhog-ftp-adapter:configuration:source-config](../evidence/sources.md#src-6674ffa9af)
  - [configuration:stove0-operator-contracts:configuration:admission-catalog](../evidence/sources.md#src-c0d7e75302)
  - [configuration:stove0-recipe-config:configuration:recipe-catalog](../evidence/sources.md#src-28686050a7)
  - [configuration:stove0-review-target-support:configuration:review-target-config](../evidence/sources.md#src-cca9387ce6)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **8**
<a id="p-3ebc61fc99"></a>
#### `extent-rule/configured-capacity/v1`

| Rule field | Value |
|---|---|
| `authority` | the source-linked operator configuration field |
| `capacity_behavior` | explicit-reject-defer-or-throttle |
| `policy` | operational_policy |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"capacity_behavior": "explicit-reject-defer-or-throttle", "silent_truncation": "forbidden"}`
- Executable authorities:
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_ARCHIVE_STORE_{store}_{setting}](../evidence/sources.md#src-3071ba44b3)
  - [configuration-environment-pattern:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_{store}_{setting}](../evidence/sources.md#src-956dbca0d9)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FILE_LOG_BYTES](../evidence/sources.md#src-283d9ea8ad)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../evidence/sources.md#src-2057e6d1cc)
  - [configuration-environment:piggity:PIGGITY_UPLOAD_FINALIZE_TIMEOUT_SECONDS](../evidence/sources.md#src-40c863880d)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-bb20193981)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_FILE_WINDOW](../evidence/sources.md#src-4629108138)
  - [configuration-environment:riverhog-client:RIVERHOG_DOWNLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-7d03d4b83f)
  - [configuration-environment:riverhog-client:RIVERHOG_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-908bba2b9b)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_CONCURRENCY](../evidence/sources.md#src-5d0adeaf5d)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_FILE_WINDOW](../evidence/sources.md#src-93e4082935)
  - [configuration-environment:riverhog-client:RIVERHOG_UPLOAD_TIMEOUT_SECONDS](../evidence/sources.md#src-6e31cc159d)
  - [configuration-environment:riverhog-ftp-adapter-api-client:RIVERHOG_FTP_ADAPTER_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-97bfcc2b77)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_CACHE_ENTRIES](../evidence/sources.md#src-d1018a4c53)
  - [configuration-environment:riverhog-server:RIVERHOG_AGE_SESSION_DERIVATION_CONCURRENCY](../evidence/sources.md#src-b3f6d318c3)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PART_PLAINTEXT_BYTES](../evidence/sources.md#src-309bc75357)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_PREPARE_CONCURRENCY](../evidence/sources.md#src-36a45a4230)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_REQUEST_CONCURRENCY](../evidence/sources.md#src-b5ab0e168b)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_UPLOAD_SWEEP_INTERVAL](../evidence/sources.md#src-19a319fc4e)
  - [configuration-environment:riverhog-server:RIVERHOG_ARCHIVE_WRITE_CONCURRENCY](../evidence/sources.md#src-66ed403dd1)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_REAP_BATCH_SIZE](../evidence/sources.md#src-e04d565c18)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_HISTORY_RETENTION](../evidence/sources.md#src-735831a8d4)
  - [configuration-environment:riverhog-server:RIVERHOG_CATALOG_SYNC_PAGE_SIZE_MAX](../evidence/sources.md#src-65433124fb)
  - [configuration-environment:riverhog-server:RIVERHOG_COLLECTION_UPLOAD_CUSTODY_LEASE](../evidence/sources.md#src-4cfc3a8358)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_REAP_BATCH_SIZE](../evidence/sources.md#src-fef401b6a9)
  - [configuration-environment:riverhog-server:RIVERHOG_EVENT_CONTEXT_RETENTION](../evidence/sources.md#src-42756a2b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-e1d4295958)
  - [configuration-environment:riverhog-server:RIVERHOG_INGRESS_SOURCE_READ_CHUNK_BYTES](../evidence/sources.md#src-bf9ecd1b0a)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_MEMBER_BYTES](../evidence/sources.md#src-f8aff5063d)
  - [configuration-environment:riverhog-server:RIVERHOG_PACK_SOURCE_BYTES](../evidence/sources.md#src-b2ca118143)
  - [configuration-environment:riverhog-server:RIVERHOG_RAW_VOLUME_PLAINTEXT_BYTES](../evidence/sources.md#src-3fb717c8c3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_NEW_ARCHIVE_LEASE](../evidence/sources.md#src-22c1f9d04c)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_SWEEP_INTERVAL](../evidence/sources.md#src-d858623918)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_CACHE_WRITE_SEGMENT_BYTES](../evidence/sources.md#src-26efc77b40)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_DEFAULT_LEASE](../evidence/sources.md#src-4fd9a92a9d)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_INFLIGHT_BYTES](../evidence/sources.md#src-cb29acf59b)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_LEASE](../evidence/sources.md#src-c44aeb9ce3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_MAX_RANGE_BYTES](../evidence/sources.md#src-1e662f4cb2)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_PENDING_TIMEOUT](../evidence/sources.md#src-01bd40bbb3)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RANGE_MERGE_GAP_BYTES](../evidence/sources.md#src-972a8351e5)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_READ_CHUNK_BYTES](../evidence/sources.md#src-236f08fcd9)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_REQUEST_CONCURRENCY](../evidence/sources.md#src-a586c2cf3e)
  - [configuration-environment:riverhog-server:RIVERHOG_RETRIEVAL_RESTORE_POLL_INTERVAL](../evidence/sources.md#src-4c5aea570f)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../evidence/sources.md#src-be41c0e7eb)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_ATTEMPTS](../evidence/sources.md#src-989751bd37)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../evidence/sources.md#src-0419b135c2)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-f9cdca0f7d)
  - [configuration-environment:riverhog-storage-adapter-aws:RIVERHOG_AWS_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../evidence/sources.md#src-47cfeed976)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_CONNECT_TIMEOUT_SECONDS](../evidence/sources.md#src-bb79933c34)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_ATTEMPTS](../evidence/sources.md#src-2072a40ae0)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_MAX_POOL_CONNECTIONS](../evidence/sources.md#src-eba038dc56)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-8a38b0e993)
  - [configuration-environment:riverhog-storage-adapter-backblaze:RIVERHOG_BACKBLAZE_STORAGE_ADAPTER_READ_TIMEOUT_SECONDS](../evidence/sources.md#src-7e10a8a388)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_MINIMUM_FREE_BYTES](../evidence/sources.md#src-3a1b9a87f7)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_READ_CHUNK_BYTES](../evidence/sources.md#src-7b65a90cf1)
  - [configuration-environment:riverhog-storage-adapter-filesystem:RIVERHOG_FILESYSTEM_STORAGE_ADAPTER_SEGMENT_BYTES](../evidence/sources.md#src-d0691ed2c9)
  - [configuration-environment:stove0-api-client:STOVE0_HTTP_TIMEOUT_SECONDS](../evidence/sources.md#src-a470f49fc7)
  - [configuration-environment:stove0-review-rclone-effect-target:STOVE0_REVIEW_RCLONE_EFFECT_TARGET_RCLONE_TIMEOUT_SECONDS](../evidence/sources.md#src-7f004c5323)
  - [configuration-environment:stove0-server:STOVE0_BROWSE_TOKEN_LIFETIME_SECONDS](../evidence/sources.md#src-8e79bc10a5)
  - [configuration-environment:stove0-server:STOVE0_CAPABILITY_TTL_SECONDS](../evidence/sources.md#src-0e9a09a14d)
  - [configuration-environment:stove0-server:STOVE0_CLAIM_LEASE_SECONDS](../evidence/sources.md#src-1997f656ba)
  - [configuration-environment:stove0-server:STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-024a6340ee)
  - [configuration-environment:stove0-server:STOVE0_SCHEDULER_INTERVAL_SECONDS](../evidence/sources.md#src-1ba44b59b5)
  - [configuration-environment:stove0-server:STOVE0_TARGET_AUTHORITY_BATCH_SIZE](../evidence/sources.md#src-86bb163042)
  - [configuration-environment:stove0-target-support:STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS](../evidence/sources.md#src-a4a7b5aed4)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)

Applications: **66**
<a id="p-75a89f9d1c"></a>
#### `extent-rule/extension-contract/v1`

| Rule field | Value |
|---|---|
| `authority` | the independently versioned extension contract |
| `core_semantic_maximum` | None |
| `policy` | extension_owned |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-attributes.json](../evidence/sources.md#src-8191cd2baf)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-flags.json](../evidence/sources.md#src-cb9d75fa32)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/darwin-file-stat.json](../evidence/sources.md#src-7295b2b671)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-file-stat.json](../evidence/sources.md#src-79a949b157)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fs-flags.json](../evidence/sources.md#src-5f8f37c8a2)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-fsxattr.json](../evidence/sources.md#src-8ed956c86a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-mount-context.json](../evidence/sources.md#src-e3afb2680c)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/linux-statx-attributes.json](../evidence/sources.md#src-ea93e415ba)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/macos-volume-context.json](../evidence/sources.md#src-86983ef410)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-backup-stream-info.json](../evidence/sources.md#src-40a5d23088)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-compression-state.json](../evidence/sources.md#src-40e7549720)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-attributes.json](../evidence/sources.md#src-01ce873e85)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-file-stat.json](../evidence/sources.md#src-232ad4e832)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-integrity-info.json](../evidence/sources.md#src-c4a2d3a892)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-usn-record.json](../evidence/sources.md#src-72281b7ad5)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-volume-context.json](../evidence/sources.md#src-025e48b0ab)

Applications: **17**
<a id="p-574724b48a"></a>
#### `extent-rule/no-semantic-maximum/v1`

| Rule field | Value |
|---|---|
| `authority` | the owning schema's deliberate absence of a semantic maximum |
| `declared_operational_maximum` | None |
| `future_capacity_behavior` | explicit-configured-reject-defer-or-throttle |
| `hidden_maximum` | forbidden |
| `policy` | operational_policy |
| `semantic_maximum` | None |
| `silent_truncation` | forbidden |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"hidden_maximum": "forbidden", "silent_truncation": "forbidden"}`
- Executable authorities:
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/observation-policy.json](../evidence/sources.md#src-9581f745b7)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/sparse-map.json](../evidence/sources.md#src-5ae7bcd86c)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)

Applications: **166**
<a id="p-6b76b527cb"></a>
#### `extent-rule/route-progression/v1`

| Rule field | Value |
|---|---|
| `authority` | the route-owned x-riverhog-read-collection declaration |
| `completion` | the owning progression contract |
| `policy` | segmented_no_total_max |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"completion": "the owning progression contract"}`
- Executable authorities:
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)

Applications: **67**
<a id="p-c0db822fc0"></a>
#### `extent-rule/schema-bound/v1`

| Rule field | Value |
|---|---|
| `authority` | the projected JSON Schema constraint |
| `exceeded` | schema-validation-error |
| `policy` | fixed-or-contract-max |
| `requirement` | a non-fixed set maximum carries an owning reason declaration |

- Applicability: `["/external_contract/extents/decisions"]`
- Observable result or violation: `{"exceeded": "schema-validation-error"}`
- Executable authorities:
  - [cli:gogurt](../evidence/sources.md#src-3b2297c37d)
  - [cli:mango-fish](../evidence/sources.md#src-3dcd5eedf2)
  - [cli:piggity](../evidence/sources.md#src-094022231f)
  - [cli:riverhog-ftp-adapter](../evidence/sources.md#src-303f765bca)
  - [cli:riverhog-recover](../evidence/sources.md#src-375119d633)
  - [cli:riverhog-storage-adapter-conformance](../evidence/sources.md#src-7ab923569b)
  - [cli:riverhog-storage-adapter-filesystem-materialize](../evidence/sources.md#src-c89790480b)
  - [cli:riverhog-storage-adapter-schemas](../evidence/sources.md#src-b90a9d08ff)
  - [cli:stove0](../evidence/sources.md#src-6203ae7d88)
  - [cli:stove0-observer-schemas](../evidence/sources.md#src-e6175e3ae2)
  - [cli:stove0-review-sampler-conformance](../evidence/sources.md#src-5796b3dff4)
  - [cli:stove0-review-sampler-schemas](../evidence/sources.md#src-a75c35f0c8)
  - [cli:stove0-target-schemas](../evidence/sources.md#src-71d64b87b5)
  - [configuration:mango-fish:configuration:mango-fish-config](../evidence/sources.md#src-fead015e98)
  - [configuration:riverhog-ftp-adapter:configuration:ftp-adapter-config](../evidence/sources.md#src-cf8c44826f)
  - [configuration:riverhog-ftp-adapter:configuration:source-config](../evidence/sources.md#src-6674ffa9af)
  - [configuration:stove0-operator-contracts:configuration:admission-catalog](../evidence/sources.md#src-c0d7e75302)
  - [configuration:stove0-recipe-config:configuration:recipe-catalog](../evidence/sources.md#src-28686050a7)
  - [configuration:stove0-review-target-support:configuration:review-target-config](../evidence/sources.md#src-cca9387ce6)
  - [extent:extent-contract](../evidence/sources.md#src-5ac94d0a12)
  - [generator:contract-projection](../evidence/sources.md#src-47381a6c4f)
  - [openapi:riverhog](../evidence/sources.md#src-c42f268fc9)
  - [openapi:riverhog-ftp-adapter](../evidence/sources.md#src-c3a51ac29a)
  - [openapi:stove0](../evidence/sources.md#src-52e6e32124)
  - [operations:operation-matrix](../evidence/sources.md#src-b032bdc56b)
  - [protocol:generated:riverhog-storage-adapter](../evidence/sources.md#src-ef281f2471)
  - [protocol:generated:stove0-observer](../evidence/sources.md#src-dcc0b5485b)
  - [protocol:generated:stove0-review-sampler](../evidence/sources.md#src-b47f3f4d7b)
  - [protocol:generated:stove0-target](../evidence/sources.md#src-2c42f9d39a)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/journal-entry.schema.json](../evidence/sources.md#src-819610ba95)
  - [protocol:https://nashspence.github.io/riverhog/v1/provenance/observers/schemas/windows-object-id.json](../evidence/sources.md#src-76f06f0f43)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../evidence/sources.md#src-fc0a3e3cab)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-terminal-v1.schema.json](../evidence/sources.md#src-c45ad44582)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../evidence/sources.md#src-c83e346f06)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json](../evidence/sources.md#src-57bde0d90d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../evidence/sources.md#src-c8e0251dd9)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-root-v1.schema.json](../evidence/sources.md#src-7c2ebe352d)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-terminal-v1.schema.json](../evidence/sources.md#src-a639df0ba1)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../evidence/sources.md#src-a3bfff3737)
  - [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-recovery-descriptor-v1.schema.json](../evidence/sources.md#src-bd3602393c)

Applications: **454**

## Exclusion

| Policy | Applications |
|---|---:|
| [exclusion/process-launcher-not-cli/v1](#p-572523784c) | 13 |
| [exclusion/python-package-no-declared-api/v1](#p-b061d0042d) | 22 |

### Definitions

<a id="p-572523784c"></a>
#### `exclusion/process-launcher-not-cli/v1`

The installed entry point starts a separately inventoried process protocol and does not expose an independently maintained human or JSON CLI.

- Applicability: `"Installed service, adapter, observer, target, sampler, and effect launchers."`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [release:release.toml](../evidence/sources.md#src-c5380dbe5f)

Applications: **13**
<a id="p-b061d0042d"></a>
#### `exclusion/python-package-no-declared-api/v1`

The installed Python package declares no explicit __all__ surface and therefore does not expose a freeze-protected Python API.

- Applicability: `"Importable packages carried by release wheels without declared exports."`
- Observable result or violation: `{"conforming_result": "the observable surface satisfies the stated meaning", "violation": "the observable surface contradicts the stated meaning"}`
- Executable authorities:
  - [python:config-validation:config_validation](../evidence/sources.md#src-47bec59a9e)
  - [python:gogurt:gogurt](../evidence/sources.md#src-11620caab6)
  - [python:mango-fish:mango_fish](../evidence/sources.md#src-f1055f88c6)
  - [python:mango-fish:mango_fish.state_migrations](../evidence/sources.md#src-96c45baf8e)
  - [python:piggity:piggity](../evidence/sources.md#src-c057ec2476)
  - [python:piggity:piggity.state_migrations](../evidence/sources.md#src-7d493ce5e0)
  - [python:riverhog-provenance-linux-observer:riverhog_provenance_linux_observer](../evidence/sources.md#src-2abaac12f4)
  - [python:riverhog-provenance-macos-observer:riverhog_provenance_macos_observer](../evidence/sources.md#src-2a56adac0b)
  - [python:riverhog-provenance-windows-observer:riverhog_provenance_windows_observer](../evidence/sources.md#src-30471eb7d3)
  - [python:riverhog-server:riverhog_api](../evidence/sources.md#src-067859de12)
  - [python:riverhog-server:riverhog_api.routers](../evidence/sources.md#src-30d4a79478)
  - [python:riverhog-server:riverhog_api.schemas](../evidence/sources.md#src-61d85b002b)
  - [python:riverhog-server:riverhog_core](../evidence/sources.md#src-fcaa6d2479)
  - [python:riverhog-server:riverhog_core.domain](../evidence/sources.md#src-7ed9f62613)
  - [python:riverhog-server:riverhog_core.ports](../evidence/sources.md#src-d1f13c410c)
  - [python:riverhog-server:riverhog_core.services](../evidence/sources.md#src-04f2db8645)
  - [python:riverhog-server:riverhog_core.state_migrations](../evidence/sources.md#src-ea08488d9c)
  - [python:riverhog-server:riverhog_core.stores](../evidence/sources.md#src-be0af349bc)
  - [python:riverhog-storage-adapter-backblaze:riverhog_storage_adapter_backblaze](../evidence/sources.md#src-18cbb8f253)
  - [python:stove0-client:stove0_cli](../evidence/sources.md#src-b2668b2370)
  - [python:stove0-server:stove0_core.state_migrations](../evidence/sources.md#src-4e7fd480de)
  - [python:time-formats:time_formats](../evidence/sources.md#src-4f4a8234ef)

Applications: **22**
