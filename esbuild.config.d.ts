import type { BuildOptions } from "esbuild";

export const extensionConfig: BuildOptions;
export const browserExtensionConfig: BuildOptions;
export const webviewConfig: BuildOptions;

export function build(): Promise<void>;
