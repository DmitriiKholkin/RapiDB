import { beforeEach } from "vitest";
import { createProjectTempDir } from "../runtime/tempDirectories";
import { createExtensionContextStub } from "./fakeConnectionManagerStore";

// All simulated extension hosts in one test share a storage domain.
export function registerConnectionStorageFixture() {
  let storagePath: string;
  beforeEach(async () => {
    storagePath = await createProjectTempDir("extension-host", "connections-");
  });
  return () => ({
    ...createExtensionContextStub(),
    globalStorageUri: { scheme: "file", fsPath: storagePath },
  });
}
