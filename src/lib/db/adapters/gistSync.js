/**
 * GitHub Gist REST API sync for better-sqlite3 adapter.
 * Handles backup/restore of the SQLite file to/from a GitHub Gist.
 *
 * Setup:
 * 1. Create a GitHub PAT with 'gist' scope at https://github.com/settings/tokens
 * 2. Set env vars: GITHUB_TOKEN (PAT), GITHUB_GIST_ID (optional — created on first backup if missing)
 */

const GITHUB_API = "https://api.github.com";

export async function fetchFromGist(gistId, token) {
  const res = await fetch(`${GITHUB_API}/gists/${gistId}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
  });
  console.log(`[gist] FETCH ← ${res.status} ${res.statusText} (gist: ${gistId})`);
  if (!res.ok) {
    if (res.status === 404) {
      console.log("[gist] FETCH → gist not found (404)");
      return null;
    }
    throw new Error(`[gist] fetch failed: ${res.status} ${res.statusText}`);
  }
  const json = await res.json();
  // Gist files: { "filename": { "content": "...", "raw_url": "..." } }
  const files = json.files || {};
  const sqliteFile = Object.values(files).find((f) => f.filename.endsWith(".sqlite") || f.filename === "9router-backup.sqlite");
  if (!sqliteFile) {
    console.log("[gist] FETCH → no SQLite file found in gist");
    return null;
  }
  // Download raw content from raw_url
  console.log(`[gist] FETCH → found file: ${sqliteFile.filename}, downloading from raw_url...`);
  const rawRes = await fetch(sqliteFile.raw_url);
  if (!rawRes.ok) {
    throw new Error(`[gist] raw download failed: ${rawRes.status}`);
  }
  const buf = await rawRes.arrayBuffer();
  const size = buf.byteLength;
  console.log(`[gist] FETCH → downloaded ${size} bytes from raw_url`);
  // Content is stored as base64 string (to preserve binary safely as JSON string).
  // Decode back to raw binary before returning.
  const base64 = Buffer.from(buf).toString("utf8");
  return Buffer.from(base64, "base64");
}

export async function uploadToGist(gistId, token, data, filename = "9router-backup.sqlite") {
  // Encode binary to base64 string — this preserves all bytes safely as a JSON string.
  const body = {
    description: "9Router SQLite backup",
    public: false,
    files: {
      [filename]: {
        content: data.toString("base64"),
      },
    },
  };
  const method = gistId ? "PATCH" : "POST";
  const url = gistId ? `${GITHUB_API}/gists/${gistId}` : `${GITHUB_API}/gists`;
  console.log(`[gist] UPLOAD → ${method} ${url} (${data.length} bytes base64)`);
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "Content-Type": "application/json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify(body),
  });
  console.log(`[gist] UPLOAD ← ${res.status} ${res.statusText}`);
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`[gist] upload failed: ${res.status} ${res.statusText} — ${err}`);
  }
  const json = await res.json();
  // Return new gist ID if this was a create (no gistId provided)
  return json.id;
}

export async function createGist(token, data, filename = "9router-backup.sqlite") {
  return uploadToGist(null, token, data, filename);
}
