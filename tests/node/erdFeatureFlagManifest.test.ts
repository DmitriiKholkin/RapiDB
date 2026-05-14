import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

function readManifest(): {
  contributes?: {
    commands?: Array<{ command?: string }>;
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
  it("declares a create command contribution", () => {
    const manifest = readManifest();
    const commands = manifest.contributes?.commands ?? [];

    expect(commands).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ command: "rapidb.create" }),
      ]),
    );
  });

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

  it("declares context menu entries for materialized views, functions, procedures, sequences, and types", () => {
    const manifest = readManifest();
    const contextEntries =
      manifest.contributes?.menus?.["view/item/context"] ?? [];

    expect(contextEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          command: "rapidb.showDDL",
          when: expect.stringContaining("viewItem == materializedView"),
        }),
        expect.objectContaining({
          command: "rapidb.showDDL",
          when: expect.stringContaining("viewItem == function"),
        }),
        expect.objectContaining({
          command: "rapidb.showDDL",
          when: expect.stringContaining("viewItem == procedure"),
        }),
        expect.objectContaining({
          command: "rapidb.showDDL",
          when: expect.stringContaining("viewItem == sequence"),
        }),
        expect.objectContaining({
          command: "rapidb.showDDL",
          when: expect.stringContaining("viewItem == type"),
        }),
      ]),
    );
  });

  it("keeps Show DDL bound to canonical viewItem kinds and excludes noDdl variants", () => {
    const manifest = readManifest();
    const contextEntries =
      manifest.contributes?.menus?.["view/item/context"] ?? [];
    const showDdlEntries = contextEntries.filter(
      (entry) => entry.command === "rapidb.showDDL",
    );

    expect(showDdlEntries.some((entry) => entry.when?.includes("_noDdl"))).toBe(
      false,
    );
    expect(showDdlEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          when: expect.stringContaining("viewItem == table"),
        }),
        expect.objectContaining({
          when: expect.stringContaining("viewItem == table_detail_index"),
        }),
      ]),
    );
  });

  it("adds Copy Name entries for noDdl viewItem variants", () => {
    const manifest = readManifest();
    const contextEntries =
      manifest.contributes?.menus?.["view/item/context"] ?? [];

    expect(contextEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          command: "rapidb.copyNodeName",
          when: expect.stringContaining("viewItem == table_noDdl"),
        }),
        expect.objectContaining({
          command: "rapidb.copyNodeName",
          when: expect.stringContaining("viewItem == table_detail_index_noDdl"),
        }),
      ]),
    );
  });

  it("wires Create context menus to create-aware connection and database view items", () => {
    const manifest = readManifest();
    const contextEntries =
      manifest.contributes?.menus?.["view/item/context"] ?? [];

    expect(contextEntries).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          command: "rapidb.create",
          when: expect.stringContaining(
            "viewItem == connectionNode_connected_canCreateDatabase",
          ),
        }),
        expect.objectContaining({
          command: "rapidb.create",
          when: expect.stringContaining(
            "viewItem == connectionNode_disconnected_canCreateDatabase",
          ),
        }),
        expect.objectContaining({
          command: "rapidb.create",
          when: expect.stringContaining("viewItem == database_canCreateSchema"),
        }),
      ]),
    );
  });

  it("keeps legacy viewItem conditions for non-create connection/database commands", () => {
    const manifest = readManifest();
    const contextEntries =
      manifest.contributes?.menus?.["view/item/context"] ?? [];

    const findWhen = (command: string) =>
      contextEntries.find((entry) => entry.command === command)?.when ?? "";

    expect(findWhen("rapidb.connect")).toContain(
      "viewItem == connectionNode_disconnected",
    );
    expect(findWhen("rapidb.disconnect")).toContain(
      "viewItem == connectionNode_connected",
    );
    expect(findWhen("rapidb.openErd")).toContain("viewItem == database");
  });
});
