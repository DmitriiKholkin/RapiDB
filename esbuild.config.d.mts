import type { BuildOptions } from "esbuild";

export const extensionConfig: BuildOptions;
export const webviewConfig: BuildOptions;
export const sqliteWorkerConfig: BuildOptions;
export function build(): Promise<void>;
