# Fulfill a fetch from optical media

The `arc-disc` CLI is the recovery client for a machine with an optical drive.

## Flow

1. Read the fetch manifest.
2. Create or resume the user-specified local temporary recovery state location.
3. Determine which disc to insert next from the manifest's part-level recovery hints.
4. Read and decrypt encrypted payloads from each required optical copy.
5. Reconstruct each logical file locally, concatenating plaintext parts in ascending part index
   order when needed.
6. Upload one final recovered plaintext file for each manifest entry.
7. Complete the fetch.

CLI example:

```bash
arc-disc fetch fx_01JV8W5J8M8F3J5V4A8Q --state-dir /var/tmp/arc/fx_01JV8W5J8M8F3J5V4A8Q --json
```

The command should exit successfully only if the fetch reaches `done`.
