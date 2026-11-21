// Builds a minimal 64-bit little-endian ELF shared object with a dynamic
// section (DT_NEEDED entries) and a .gnu.version_r section, enough for
// scripts/sqlite-elf-requirements.mjs.
export const ELF_MACHINE = { x64: 62, arm64: 183 } as const;

export function buildElfSharedObject({
  machine = ELF_MACHINE.x64,
  needed = [],
  versions = {},
}: {
  machine?: number;
  needed?: string[];
  versions?: Record<string, string[]>;
} = {}): Buffer {
  const offsets = new Map<string, number>();
  const strings: Buffer[] = [Buffer.from([0])];
  let stringSize = 1;
  const str = (value: string): number => {
    let offset = offsets.get(value);
    if (offset === undefined) {
      offset = stringSize;
      offsets.set(value, offset);
      const bytes = Buffer.from(`${value}\0`, "latin1");
      strings.push(bytes);
      stringSize += bytes.length;
    }
    return offset;
  };

  const dynamic: Buffer[] = [];
  for (const library of needed) {
    const entry = Buffer.alloc(16);
    entry.writeBigUInt64LE(1n, 0);
    entry.writeBigUInt64LE(BigInt(str(library)), 8);
    dynamic.push(entry);
  }
  dynamic.push(Buffer.alloc(16));

  const files = Object.entries(versions);
  const verneed: Buffer[] = [];
  files.forEach(([file, names], fileIndex) => {
    const header = Buffer.alloc(16);
    header.writeUInt16LE(1, 0);
    header.writeUInt16LE(names.length, 2);
    header.writeUInt32LE(str(file), 4);
    header.writeUInt32LE(16, 8);
    header.writeUInt32LE(
      fileIndex === files.length - 1 ? 0 : 16 + 16 * names.length,
      12,
    );
    verneed.push(header);
    names.forEach((name, nameIndex) => {
      const aux = Buffer.alloc(16);
      aux.writeUInt32LE(str(name), 8);
      aux.writeUInt32LE(nameIndex === names.length - 1 ? 0 : 16, 12);
      verneed.push(aux);
    });
  });

  const align = (value: number) => Math.ceil(value / 8) * 8;
  const dynstr = Buffer.concat(strings);
  const dynstrOffset = 64;
  const dynamicOffset = align(dynstrOffset + dynstr.length);
  const dynamicBytes = Buffer.concat(dynamic);
  const verneedOffset = dynamicOffset + dynamicBytes.length;
  const verneedBytes = Buffer.concat(verneed);
  const sectionOffset = align(verneedOffset + verneedBytes.length);
  const sections: {
    type: number;
    offset: number;
    size: number;
    link: number;
    info: number;
  }[] = [
    { type: 0, offset: 0, size: 0, link: 0, info: 0 },
    { type: 3, offset: dynstrOffset, size: dynstr.length, link: 0, info: 0 },
    {
      type: 6,
      offset: dynamicOffset,
      size: dynamicBytes.length,
      link: 1,
      info: 0,
    },
  ];
  if (files.length > 0)
    sections.push({
      type: 0x6ffffffe,
      offset: verneedOffset,
      size: verneedBytes.length,
      link: 1,
      info: files.length,
    });

  const header = Buffer.alloc(64);
  header.writeUInt32BE(0x7f454c46, 0);
  header[4] = 2;
  header[5] = 1;
  header[6] = 1;
  header.writeUInt16LE(3, 16);
  header.writeUInt16LE(machine, 18);
  header.writeUInt32LE(1, 20);
  header.writeBigUInt64LE(BigInt(sectionOffset), 0x28);
  header.writeUInt16LE(64, 0x34);
  header.writeUInt16LE(64, 0x3a);
  header.writeUInt16LE(sections.length, 0x3c);

  const table = Buffer.alloc(64 * sections.length);
  sections.forEach((section, index) => {
    const base = index * 64;
    table.writeUInt32LE(section.type, base + 4);
    table.writeBigUInt64LE(BigInt(section.offset), base + 0x18);
    table.writeBigUInt64LE(BigInt(section.size), base + 0x20);
    table.writeUInt32LE(section.link, base + 0x28);
    table.writeUInt32LE(section.info, base + 0x2c);
  });

  const image = Buffer.alloc(sectionOffset + table.length);
  header.copy(image, 0);
  dynstr.copy(image, dynstrOffset);
  dynamicBytes.copy(image, dynamicOffset);
  verneedBytes.copy(image, verneedOffset);
  table.copy(image, sectionOffset);
  return image;
}

export function glibcPrebuild(
  arch: "x64" | "arm64",
  glibc = "2.34",
  glibcxx = "3.4.29",
): Buffer {
  return buildElfSharedObject({
    machine: ELF_MACHINE[arch],
    needed: ["libc.so.6", "libm.so.6", "libstdc++.so.6"],
    versions: {
      "libc.so.6": ["GLIBC_2.17", "GLIBC_2.28", `GLIBC_${glibc}`],
      "libm.so.6": ["GLIBC_2.29"],
      "libstdc++.so.6": [
        "CXXABI_1.3.9",
        "GLIBCXX_3.4.21",
        `GLIBCXX_${glibcxx}`,
      ],
    },
  });
}

export function muslPrebuild(arch: "x64" | "arm64"): Buffer {
  return buildElfSharedObject({
    machine: ELF_MACHINE[arch],
    needed: [
      arch === "x64" ? "libc.musl-x86_64.so.1" : "libc.musl-aarch64.so.1",
      "libstdc++.so.6",
    ],
  });
}
