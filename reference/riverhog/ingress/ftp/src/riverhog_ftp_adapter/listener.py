"""Success-qualified FTP listener for the maintained reference adapter."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

from pyftpdlib.authorizers import DummyAuthorizer  # type: ignore[import-untyped]
from pyftpdlib.filesystems import (  # type: ignore[import-untyped]
    AbstractedFS,
    FilesystemError,
)
from pyftpdlib.handlers import FTPHandler  # type: ignore[import-untyped]
from pyftpdlib.servers import FTPServer  # type: ignore[import-untyped]

from riverhog_ftp_adapter.completion import CONTROL_DIR, CompletionHandoff


class _IntakeFilesystem(AbstractedFS):  # type: ignore[misc]
    """Keep adapter custody state outside the FTP-visible namespace."""

    def listdir(self, path: str) -> list[str]:
        return [entry for entry in super().listdir(path) if entry != CONTROL_DIR]

    def ftp2fs(self, ftppath: str) -> str:
        resolved = super().ftp2fs(ftppath)
        try:
            relative = Path(resolved).resolve().relative_to(Path(self.root).resolve())
        except ValueError as exc:
            raise FilesystemError("path escapes the FTP intake root") from exc
        if CONTROL_DIR in relative.parts:
            raise FilesystemError("path is reserved for adapter custody")
        return cast(str, resolved)


class _CompletionHandler(FTPHandler):  # type: ignore[misc]
    handoff: CompletionHandoff
    active_uploads: set[str]

    def ftp_STOR(self, file: str, mode: str = "w") -> str | None:
        if file in self.active_uploads:
            self.respond("450 Another transfer owns this pathname.")
            return None
        if self.handoff.has_pending_sidecar(Path(file)):
            self.respond("450 This provenance sidecar already awaits its payload.")
            return None
        result = super().ftp_STOR(file, mode)
        if result is not None:
            self.active_uploads.add(file)
        return cast(str | None, result)

    def ftp_DELE(self, path: str) -> str | None:
        if self.active_uploads:
            self.respond("450 Mutation is deferred while uploads are active.")
            return None
        return cast(str | None, super().ftp_DELE(path))

    def ftp_RNFR(self, path: str) -> str | None:
        if self.active_uploads:
            self.respond("450 Mutation is deferred while uploads are active.")
            return None
        return cast(str | None, super().ftp_RNFR(path))

    def ftp_RNTO(self, path: str) -> None:
        if self.active_uploads:
            self.respond("450 Mutation is deferred while uploads are active.")
            return
        super().ftp_RNTO(path)

    def ftp_RMD(self, path: str) -> None:
        if self.active_uploads:
            self.respond("450 Mutation is deferred while uploads are active.")
            return
        super().ftp_RMD(path)

    def respond(self, resp: str, logfun=None) -> None:  # type: ignore[no-untyped-def]
        channel = self.data_channel
        if (
            resp.startswith("226 ")
            and channel is not None
            and channel.receive
            and channel.transfer_finished
        ):
            self._riverhog_success_response = (resp, logfun)
            return
        if logfun is None:
            super().respond(resp)
        else:
            super().respond(resp, logfun=logfun)

    def on_file_received(self, file: str) -> None:
        pending = getattr(self, "_riverhog_success_response", None)
        if pending is None:
            self.respond("451 Completed transfer lacked a durable handoff boundary.")
            return
        del self._riverhog_success_response
        try:
            self.handoff.complete(Path(file))
        except Exception:
            self.log_exception(self)
            super().respond("451 Completed transfer could not enter durable custody.")
        else:
            response, logfun = pending
            super().respond(response, logfun=logfun)
        finally:
            self.active_uploads.discard(file)

    def on_incomplete_file_received(self, file: str) -> None:
        self.active_uploads.discard(file)


def build_ftp_server(
    *,
    source_root: Path,
    source_id: str,
    username: str,
    password: str,
    host: str,
    port: int,
    passive_ports: Sequence[int],
    public_host: str | None,
    max_connections: int,
    max_connections_per_ip: int,
) -> Any:
    handoff = CompletionHandoff(source_root, source_id)
    authorizer = DummyAuthorizer()
    authorizer.add_user(username, password, str(source_root), perm="elradfmwMT")
    handler = cast(Any, type("RiverhogFtpHandler", (_CompletionHandler,), {}))
    handler.authorizer = authorizer
    handler.abstracted_fs = _IntakeFilesystem
    handler.handoff = handoff
    handler.active_uploads = set()
    handler.passive_ports = list(passive_ports)
    handler.masquerade_address = public_host
    handler.banner = "Riverhog FTP reference ready."
    server = FTPServer((host, port), handler)
    server.max_cons = max_connections
    server.max_cons_per_ip = max_connections_per_ip
    return server


def serve_ftp(
    *,
    source_root: Path,
    source_id: str,
    username: str,
    password: str,
    host: str,
    port: int,
    passive_ports: Sequence[int],
    public_host: str | None,
    max_connections: int,
    max_connections_per_ip: int,
) -> None:
    server = build_ftp_server(
        source_root=source_root,
        source_id=source_id,
        username=username,
        password=password,
        host=host,
        port=port,
        passive_ports=passive_ports,
        public_host=public_host,
        max_connections=max_connections,
        max_connections_per_ip=max_connections_per_ip,
    )
    server.serve_forever()


__all__ = ["build_ftp_server", "serve_ftp"]
