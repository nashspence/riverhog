export function createWorker(entries) {
  const files = new Map(entries);

  return {
    async fetch(request, env) {
      if (!["GET", "HEAD"].includes(request.method)) {
        return new Response(null, { status: 405, headers: { Allow: "GET, HEAD" } });
      }
      const url = new URL(request.url);
      let path;
      try {
        path = decodeURIComponent(url.pathname);
      } catch {
        return new Response(null, { status: 400 });
      }
      if (
        /[\u0000-\u001f\\]/.test(path) ||
        path.split("/").some((part) => part === "." || part === "..")
      ) {
        return new Response(null, { status: 400 });
      }
      const mount = env.SITE_MOUNT;
      if (path === "/" && mount !== "/") {
        url.pathname = mount;
        return Response.redirect(url.toString(), 302);
      }
      if (!path.startsWith(mount)) {
        return new Response(null, { status: 404 });
      }
      let name = path.slice(mount.length);
      if (!name || name.endsWith("/")) name += "index.html";
      let entry = files.get(name);
      if (!entry && files.has(name + "/index.html")) {
        url.pathname += "/";
        return Response.redirect(url.toString(), 302);
      }
      if (!entry) return new Response(null, { status: 404 });
      const [digest, size, type] = entry;
      const etag = '"' + digest + '"';
      const headers = new Headers({
        "Content-Type": type,
        "Cache-Control": "public, max-age=0, must-revalidate",
        ETag: etag,
        "X-Content-Type-Options": "nosniff",
      });
      const condition = request.headers.get("If-None-Match");
      if (
        condition &&
        condition.split(",").some((value) => {
          value = value.trim().replace(/^W\//, "");
          return value === "*" || value === etag;
        })
      ) {
        return new Response(null, { status: 304, headers });
      }
      let response;
      try {
        response = await fetch(env.CONTENT_BASE_URL + digest, {
          method: request.method,
          redirect: "follow",
          headers: {
            "Accept-Encoding": "identity",
            "User-Agent": "riverhog-documentation/1.0",
          },
        });
      } catch {
        return new Response(null, { status: 502 });
      }
      if (
        response.status !== 200 ||
        (response.headers.has("Content-Length") &&
          Number(response.headers.get("Content-Length")) !== size)
      ) {
        if (response.body) await response.body.cancel();
        return new Response(null, { status: 502 });
      }
      headers.set("Content-Length", String(size));
      return new Response(request.method === "HEAD" ? null : response.body, {
        status: 200,
        headers,
      });
    },
  };
}
