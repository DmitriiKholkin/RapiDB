import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

function readManifest(): {
  contributes?: {
    configuration?: {
      properties?: Record<string, { default?: unknown }>;
    };
    menus?: Record<string, Array<{ command?: string; when?: string }>>;
  };
} {
  return JSON.parse(
    readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
  ) as ReturnType<typeof readManifest>;
}

describe("ERD manifest", () => {
  it("does not declare an ERD enable setting", () => {
    const manifest = readManifest();

    expect(
      manifest.contributes?.configuration?.properties?.["rapidb.erd.enabled"],
    ).toBeUndefined();
  });

  it("shows ERD only for database and schema context menus", () => {
    const manifest = readManifest();
    const menuBuckets = manifest.contributes?.menus ?? {};
    const erdContextEntries = (menuBuckets["view/item/context"] ?? []).filter(
      (entry) => entry.command === "rapidb.openErd",
    );

    expect(erdContextEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          when: expect.stringContaining("viewItem == database"),
        }),
        expect.objectContaining({
          when: expect.stringContaining("viewItem == schema"),
        }),
      ]),
    );
    expect(erdContextEntries.length).toBe(2);
    expect(
      erdContextEntries.every(
        (entry) => !entry.when?.includes("config.rapidb.erd.enabled"),
      ),
    ).toBe(true);
  });
});
