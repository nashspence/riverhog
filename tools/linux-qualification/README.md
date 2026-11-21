# Linux qualification environment

This is a development runner for the repository's ordinary
`make linux-qualification`, not a Riverhog application image or a Docker daemon.
It contains Linux bootstrap dependencies. Run `mise trust`, `mise install --locked`
and the unchanged Make target in the checked-out repository; `mise.lock` and
`uv.lock` continue to select the project tools and Python dependencies. The image
does not contain a checkout, credentials, a virtual environment or installed
project dependencies.

Build from the repository root:

```sh
docker buildx build --load --tag riverhog-linux-qualification:dev tools/linux-qualification
docker run --rm --network=none --cap-drop=ALL --security-opt=no-new-privileges \
  --user=1000:1000 --entrypoint=/usr/local/libexec/riverhog-qualification-check \
  riverhog-linux-qualification:dev
```

The second command checks image contents. It is not a full qualification run.
Docker CLI, Buildx and Compose match the CI tool versions. Mise is a bootstrap
executable; repository tools are installed from the checkout's lock rather than
copied from a developer's machine. External image inputs are versioned and
digest-pinned; Ubuntu packages receive distribution updates when rebuilt.

## Runtime requirements

An image supplies binaries, not the Linux runtime conditions the native tests
exercise. The execution environment must provide:

- Linux with cgroup v2 and a real writable, delegated subtree for the
  unprivileged test user's systemd manager and services. A read-only cgroup tree
  or a mocked `systemctl` is insufficient.
- A real systemd user manager with its matching `XDG_RUNTIME_DIR` and
  `DBUS_SESSION_BUS_ADDRESS`. The default entrypoint runs systemd as container
  PID 1; run qualification as the unprivileged `ubuntu` user (UID/GID 1000), not
  as PID 1's root identity. Starting PID 1 alone does not create that user session.
- Real mount/unmount activity for native installation tests. Where default
  mount setup is unavailable, supply the existing
  `RIVERHOG_QUALIFICATION_MOUNT_FIXTURE` interface exposed by `make help` and
  consumed by `scripts/qualify_installation.py`. Do not replace mounts with
  directory creation or skip the native checks.
- Kernel and namespace/security policy support for the selected runtime and
  mount fixture, including unprivileged user/mount namespaces when the provider
  uses them. Installing namespace utilities does not grant those permissions.
- A disposable Docker endpoint supporting Buildx/Bake, image loading and
  inspection, Compose, health checks, named volumes and published loopback ports.
  The qualification process must be able to reach those published ports. Remote
  daemon bind-mount paths must refer to the same checkout and scratch data.
- Network access to the pinned package, image and conformance-fixture sources,
  and writable disk-backed checkout, home and scratch directories. Executable
  temporary files, ownership changes and required filesystem operations must
  work; a small memory-backed temporary filesystem is unsuitable for image and
  dependency storage.

The runtime provider owns namespace/seccomp policy, cgroup delegation, mount
fixtures, Docker connectivity, resource ceilings and cleanup. This image neither
installs a host helper nor requests privileged mode, host namespaces, a host
Docker socket or additional capabilities. `sudo` is available for the native
test's existing mount fallback; no passwordless sudo rule is installed.

`make linux-qualification` retains its default concurrency and deadlines.
Resource needs depend on the engine, storage and competing work; there is no
established universal minimum CPU or RAM requirement. Missing runtime conditions
must be reported as unavailable qualification, not successful coverage.
