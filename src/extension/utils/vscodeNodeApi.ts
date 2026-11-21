// Node.js versions embedded in VS Code releases that RapiDB supports, oldest
// first. Source: the VS Code release notes ("Engineering" / Electron update)
// and https://releases.electronjs.org for the Electron → Node.js mapping.
// Add a row when engines.vscode in package.json is raised.
export const VSCODE_NODE_VERSIONS: readonly {
  vscode: string;
  electron: string;
  node: string;
}[] = [{ vscode: "1.101.0", electron: "35.5.1", node: "22.15.1" }];

// First Node.js release of each major line that provides a Node-API version
// (that version and all later releases of the line provide it). Source:
// https://nodejs.org/api/n-api.html#node-api-version-matrix
export const NODE_API_VERSION_MATRIX: readonly {
  nodeApi: number;
  firstReleases: readonly string[];
  allFromMajor: number;
}[] = [
  {
    nodeApi: 8,
    firstReleases: ["12.22.0", "14.17.0", "15.12.0"],
    allFromMajor: 16,
  },
  { nodeApi: 9, firstReleases: ["18.17.0", "20.3.0"], allFromMajor: 21 },
  { nodeApi: 10, firstReleases: ["22.14.0", "23.6.0"], allFromMajor: 24 },
];

function versionParts(version: string): number[] {
  return version
    .replace(/^v/, "")
    .split(".")
    .map((part) => Number.parseInt(part, 10));
}

function compareVersions(left: string, right: string): number {
  const a = versionParts(left);
  const b = versionParts(right);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

// Highest Node-API version a Node.js release provides according to the
// matrix above (0 if it predates the matrix).
export function nodeApiVersionOfNode(nodeVersion: string): number {
  const [major] = versionParts(nodeVersion);
  let supported = 0;
  for (const row of NODE_API_VERSION_MATRIX) {
    const sameLine = row.firstReleases.find(
      (release) => versionParts(release)[0] === major,
    );
    if (
      major >= row.allFromMajor ||
      (sameLine !== undefined && compareVersions(nodeVersion, sameLine) >= 0)
    ) {
      supported = Math.max(supported, row.nodeApi);
    }
  }
  return supported;
}

export function oldestVSCodeWithNodeApi(nodeApi: number): string | null {
  const release = VSCODE_NODE_VERSIONS.find(
    (entry) => nodeApiVersionOfNode(entry.node) >= nodeApi,
  );
  return release ? release.vscode.replace(/\.0$/, "") : null;
}
