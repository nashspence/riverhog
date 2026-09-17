# riverhog-age: Python

[Atlas](../../../index.md) · [Authority](../index.md) · [Policies](../../../policies/index.md)

Declared public imports and their selected exact structural contracts.

## Contract elements

### `riverhog_age`

- [AEAD_TAG_SIZE](riverhog-age-aead-tag-size.md)
- [AgeAlignedUnitPlan](riverhog-age-agealignedunitplan.md)
  - [ciphertext_len](riverhog-age-agealignedunitplan-ciphertext-len.md)
  - [plaintext_len](riverhog-age-agealignedunitplan-plaintext-len.md)
- [AgeDecryptError](riverhog-age-agedecrypterror.md)
- [AgeFormatError](riverhog-age-ageformaterror.md)
- [CHUNK_SIZE](riverhog-age-chunk-size.md)
- [DEFAULT_CHUNKS_PER_AGE_UNIT](riverhog-age-default-chunks-per-age-unit.md)
- [DEFAULT_SCRYPT_LOG_N](riverhog-age-default-scrypt-log-n.md)
- [PAYLOAD_NONCE_SIZE](riverhog-age-payload-nonce-size.md)
- [ResumableAgeScryptSession](riverhog-age-resumableagescryptsession.md)
  - [age_aligned_unit_plans](riverhog-age-resumableagescryptsession-age-aligned-unit-plans.md)
  - [age_prefix](riverhog-age-resumableagescryptsession-age-prefix.md)
  - [create](riverhog-age-resumableagescryptsession-create.md)
  - [decrypt_chunk](riverhog-age-resumableagescryptsession-decrypt-chunk.md)
  - [encrypt_chunk](riverhog-age-resumableagescryptsession-encrypt-chunk.md)
  - [encrypt_part](riverhog-age-resumableagescryptsession-encrypt-part.md)
  - [encrypt_plaintext](riverhog-age-resumableagescryptsession-encrypt-plaintext.md)
  - [export_state](riverhog-age-resumableagescryptsession-export-state.md)
  - [from_state](riverhog-age-resumableagescryptsession-from-state.md)
- [UploadState](riverhog-age-uploadstate.md)
  - [from_json_bytes](riverhog-age-uploadstate-from-json-bytes.md)
  - [to_json_bytes](riverhog-age-uploadstate-to-json-bytes.md)
- [age_chunk_count_for_plaintext_len](riverhog-age-age-chunk-count-for-plaintext-len.md)
- [age_ciphertext_len_for_plaintext_len](riverhog-age-age-ciphertext-len-for-plaintext-len.md)
- [decrypt_age_scrypt](riverhog-age-decrypt-age-scrypt.md)
- [encrypt_age_scrypt](riverhog-age-encrypt-age-scrypt.md)
- [iter_decrypt_age_scrypt](riverhog-age-iter-decrypt-age-scrypt.md)
- [iter_decrypt_payload_chunks](riverhog-age-iter-decrypt-payload-chunks.md)
- [make_age_aligned_unit_plans](riverhog-age-make-age-aligned-unit-plans.md)
- [parse_scrypt_header](riverhog-age-parse-scrypt-header.md)
- [parse_scrypt_header_from_age_file](riverhog-age-parse-scrypt-header-from-age-file.md)
- [plaintext_bytes_for_ciphertext_offset](riverhog-age-plaintext-bytes-for-ciphertext-offset.md)
- [split_plaintext_chunks](riverhog-age-split-plaintext-chunks.md)
