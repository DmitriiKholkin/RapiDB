import { existsSync, readFileSync } from "node:fs";
import { extname } from "node:path";

/**
 * Validate that a compiled binary is compatible with Node.js.
 * For N-API binaries, validation relies on Node.js being able to load the binary.
 * For non-N-API binaries, we extract MODULE_VERSION from the binary.
 */
export function validateBinaryModuleVersion(
  binaryPath,
  expectedModuleVersion = null,
  targetNodeVersion = "unspecified",
) {
  if (!existsSync(binaryPath)) {
    return {
      isValid: false,
      actualVersion: null,
      reason: `Binary file not found: ${binaryPath}`,
    };
  }

  if (extname(binaryPath) !== ".node") {
    return {
      isValid: false,
      actualVersion: null,
      reason: `File is not a .node binary: ${binaryPath}`,
    };
  }

  try {
    const buffer = readFileSync(binaryPath);

    if (buffer.length === 0) {
      return {
        isValid: false,
        actualVersion: null,
        reason: `Binary file is empty: ${binaryPath}`,
      };
    }

    const bufferStr = buffer.toString("latin1");
    const versionMatch = bufferStr.match(/node_module_version_(\d+)/);

    if (versionMatch) {
      const actualVersion = parseInt(versionMatch[1], 10);

      if (
        expectedModuleVersion !== null &&
        actualVersion !== expectedModuleVersion
      ) {
        return {
          isValid: false,
          actualVersion,
          reason: `MODULE_VERSION mismatch: expected ${expectedModuleVersion} (Node.js ${targetNodeVersion}), but got ${actualVersion}. The binary was compiled for a different Node.js version.`,
        };
      }

      return {
        isValid: true,
        actualVersion,
        reason: `Binary MODULE_VERSION=${actualVersion} is compatible with Node.js ${targetNodeVersion}`,
      };
    }

    // N-API binaries don't have the MODULE_VERSION string marker.
    return {
      isValid: true,
      actualVersion: null,
      reason: `Binary is N-API-based (compatible with Node.js 18-26+). Runtime validation by Node.js required.`,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      isValid: false,
      actualVersion: null,
      reason: `Failed to validate binary ${binaryPath}: ${message}`,
    };
  }
}

/**
 * Utility function to extract MODULE_VERSION from binary (best-effort).
 * Only works for non-N-API binaries that have the string marker.
 */
export function extractModuleVersion(binaryPath) {
  try {
    if (!existsSync(binaryPath) || extname(binaryPath) !== ".node") {
      return {
        isValid: false,
        actualVersion: null,
        actualNapiVersion: null,
        reason: `File is not a .node binary: ${binaryPath}`,
      };
    }

    const buffer = readFileSync(binaryPath);
    const bufferStr = buffer.toString("latin1");

    const versionMatch = bufferStr.match(/node_module_version_(\d+)/);
    if (!versionMatch) {
      return {
        isValid: false,
        actualVersion: null,
        actualNapiVersion: null,
        reason: `Could not extract MODULE_VERSION string from binary. This is normal for N-API binaries.`,
      };
    }

    const actualVersion = parseInt(versionMatch[1], 10);
    const napiMatch = bufferStr.match(/napi_version_(\d+)/);
    const actualNapiVersion = napiMatch ? parseInt(napiMatch[1], 10) : null;

    return {
      isValid: true,
      actualVersion,
      actualNapiVersion,
      reason: `Extracted MODULE_VERSION=${actualVersion}${actualNapiVersion !== null ? `, N-API v${actualNapiVersion}` : ""} from ${binaryPath}`,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return {
      isValid: false,
      actualVersion: null,
      actualNapiVersion: null,
      reason: `Failed to read binary file ${binaryPath}: ${message}`,
    };
  }
}

export default { extractModuleVersion, validateBinaryModuleVersion };
