/**
 * Connection Icons — SVG-иконки для отображения состояния соединений.
 *
 * Извлечены из ConnectionProvider для соблюдения SRP и уменьшения размера файла.
 * Иконки кешируются по цвету и состоянию для оптимальной производительности.
 */

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as vscode from "vscode";

const coloredIconCache = new Map<string, vscode.Uri>();

/**
 * Создаёт SVG-иконку сервера с указанным цветом и состоянием.
 * Результат кешируется для повторного использования.
 */
export function getColoredServerIconUri(
  hexColor: string,
  isConnected: boolean,
): vscode.Uri {
  const safeHex = /^#[0-9a-fA-F]{3,8}$/.test(hexColor) ? hexColor : "#888888";
  const stateKey = isConnected ? "connected" : "disconnected";
  const cacheKey = `${safeHex}:${stateKey}`;
  const cached = coloredIconCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const safeKey = safeHex.replace("#", "");
  const svgContent = isConnected
    ? buildConnectedSvg(safeHex)
    : buildDisconnectedSvg(safeHex);

  const dir = path.join(os.tmpdir(), "rapidb-icons");
  try {
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, `conn-${safeKey}-${stateKey}.svg`);
    fs.writeFileSync(filePath, svgContent, "utf8");
    const uri = vscode.Uri.file(filePath);
    coloredIconCache.set(cacheKey, uri);
    return uri;
  } catch {
    const fallback = vscode.Uri.file(
      path.join(dir, `conn-${safeKey}-${stateKey}.svg`),
    );
    coloredIconCache.set(cacheKey, fallback);
    return fallback;
  }
}

function buildDisconnectedSvg(hexColor: string): string {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" width="16" height="16">
    <path transform="translate(0,0.5)" fill="${hexColor}" d="M12 11.5a.5.5 0 1 1-1 0a.5.5 0 0 1 1 0M11.5 8a.5.5 0 1 0 0-1a.5.5 0 0 0 0 1M14 4.5c-.001.37-.14.727-.39 1c.25.273.389.63.39 1v2c-.001.37-.14.727-.39 1c.25.273.389.63.39 1v2a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 12.5v-2c.001-.37.14-.727.39-1a1.5 1.5 0 0 1-.39-1v-2c.001-.37.14-.727.39-1a1.5 1.5 0 0 1-.39-1v-2A1.5 1.5 0 0 1 3.5 1h9A1.5 1.5 0 0 1 14 2.5zm-11 0a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5v-2a.5.5 0 0 0-.5-.5h-9a.5.5 0 0 0-.5.5zM12.5 6h-9a.5.5 0 0 0-.5.5v2a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5v-2a.5.5 0 0 0-.5-.5m.5 4.5a.5.5 0 0 0-.5-.5h-9a.5.5 0 0 0-.5.5v2a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5zM11.5 4a.5.5 0 1 0 0-1a.5.5 0 0 0 0 1"/>
  </svg>`;
}

function buildConnectedSvg(hexColor: string): string {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16" width="16" height="16">
    <defs>
      <mask id="mask">
        <rect width="100%" height="100%" fill="black"/>
        <path transform="translate(0,0.5)" fill="white" d="M3.5 1h9A1.5 1.5 0 0 1 14 2.5v2c-.25.273-.389.63-.39 1c.001.37.14.727.39 1v2c-.25.273-.389.63-.39 1c.001.37.14.727.39 1v2a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 12.5v-2c.25-.273.389-.63.39-1c-.001-.37-.14-.727-.39-1v-2c.25-.273.389-.63.39-1c-.001-.37-.14-.727-.39-1v-2A1.5 1.5 0 0 1 3.5 1z"/>
        <path transform="translate(0,0.5)" fill="black" fill-rule="evenodd" d="M14 4.5c-.001.37-.14.727-.39 1c.25.273.389.63.39 1v2c-.001.37-.14.727-.39 1c.25.273.389.63.39 1v2a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 12.5v-2c.001-.37.14-.727.39-1a1.5 1.5 0 0 1-.39-1v-2c.001-.37.14-.727.39-1a1.5 1.5 0 0 1-.39-1v-2A1.5 1.5 0 0 1 3.5 1h9A1.5 1.5 0 0 1 14 2.5zm-11 0a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5v-2a.5.5 0 0 0-.5-.5h-9a.5.5 0 0 0-.5.5zM12.5 6h-9a.5.5 0 0 0-.5.5v2a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5v-2a.5.5 0 0 0-.5-.5m.5 4.5a.5.5 0 0 0-.5-.5h-9a.5.5 0 0 0-.5.5v2a.5.5 0 0 0 .5.5h9a.5.5 0 0 0 .5-.5z"/>
        <g transform="translate(0,0.5)" fill="black">
          <circle cx="11.5" cy="11.5" r="0.5"/>
          <circle cx="11.5" cy="7.5" r="0.5"/>
          <circle cx="11.5" cy="3.5" r="0.5"/>
        </g>
      </mask>
    </defs>
    <path transform="translate(0,0.5)" mask="url(#mask)" fill="${hexColor}" d="M3.5 1h9A1.5 1.5 0 0 1 14 2.5v2c-.25.273-.389.63-.39 1c.001.37.14.727.39 1v2c-.25.273-.389.63-.39 1c.001.37.14.727.39 1v2a1.5 1.5 0 0 1-1.5 1.5h-9A1.5 1.5 0 0 1 2 12.5v-2c.25-.273.389-.63.39-1c-.001-.37-.14-.727-.39-1v-2c.25-.273.389-.63.39-1c-.001-.37-.14-.727-.39-1v-2A1.5 1.5 0 0 1 3.5 1z"/>
  </svg>`;
}
