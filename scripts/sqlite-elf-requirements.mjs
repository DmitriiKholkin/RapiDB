// Reads the shared-library and symbol-version requirements of a Linux ELF
// shared object (a better-sqlite3 prebuild) without running it or any
// external tool. Only well-formed 64-bit little-endian ELF files are accepted;
// anything unexpected throws so that pin maintenance fails closed.

const SHT_STRTAB = 3;
const SHT_DYNAMIC = 6;
const SHT_GNU_VERNEED = 0x6ffffffe;
const DT_NULL = 0;
const DT_NEEDED = 1;
const ET_DYN = 3;
const SUPPORTED_MACHINES = new Map([
  [62, "x64"],
  [183, "arm64"],
]);
const MAX_VERNEED_ENTRIES = 4096;

function fail(detail) {
  throw new Error(`Malformed or unsupported ELF prebuild: ${detail}.`);
}

function checkedRange(bytes, offset, length, what) {
  if (
    !Number.isSafeInteger(offset) ||
    !Number.isSafeInteger(length) ||
    offset < 0 ||
    length < 0 ||
    offset + length > bytes.length
  )
    fail(`${what} lies outside the file`);
}

function u64(bytes, offset, what) {
  checkedRange(bytes, offset, 8, what);
  const value = bytes.readBigUInt64LE(offset);
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) fail(`${what} is too large`);
  return Number(value);
}

function u32(bytes, offset, what) {
  checkedRange(bytes, offset, 4, what);
  return bytes.readUInt32LE(offset);
}

function u16(bytes, offset, what) {
  checkedRange(bytes, offset, 2, what);
  return bytes.readUInt16LE(offset);
}

function cString(bytes, table, index, what) {
  if (index >= table.size) fail(`${what} name is outside its string table`);
  const start = table.offset + index;
  const end = bytes.indexOf(0, start);
  if (end < 0 || end >= table.offset + table.size)
    fail(`${what} name is not terminated`);
  return bytes.toString("latin1", start, end);
}

function readSections(bytes) {
  checkedRange(bytes, 0, 64, "ELF header");
  if (bytes.readUInt32BE(0) !== 0x7f454c46) fail("missing ELF magic");
  if (bytes[4] !== 2) fail("not a 64-bit ELF file");
  if (bytes[5] !== 1) fail("not a little-endian ELF file");
  if (u16(bytes, 16, "e_type") !== ET_DYN) fail("not a shared object");
  const machine = SUPPORTED_MACHINES.get(u16(bytes, 18, "e_machine"));
  if (!machine) fail("unsupported machine");
  const shoff = u64(bytes, 0x28, "e_shoff");
  const shentsize = u16(bytes, 0x3a, "e_shentsize");
  const shnum = u16(bytes, 0x3c, "e_shnum");
  if (shoff === 0 || shnum === 0 || shentsize !== 64)
    fail("missing section headers");
  checkedRange(bytes, shoff, shnum * shentsize, "section header table");
  const sections = [];
  for (let index = 0; index < shnum; index += 1) {
    const base = shoff + index * shentsize;
    const section = {
      type: u32(bytes, base + 4, "sh_type"),
      offset: u64(bytes, base + 0x18, "sh_offset"),
      size: u64(bytes, base + 0x20, "sh_size"),
      link: u32(bytes, base + 0x28, "sh_link"),
      info: u32(bytes, base + 0x2c, "sh_info"),
    };
    if (section.type !== 8 /* SHT_NOBITS */)
      checkedRange(bytes, section.offset, section.size, `section ${index}`);
    sections.push(section);
  }
  return { machine, sections };
}

function linkedStringTable(sections, section, what) {
  const table = sections[section.link];
  if (!table || table.type !== SHT_STRTAB)
    fail(`${what} does not link to a string table`);
  return table;
}

function compareVersions(a, b) {
  const left = a.split(".").map(Number);
  const right = b.split(".").map(Number);
  for (let index = 0; index < Math.max(left.length, right.length); index += 1) {
    const difference = (left[index] ?? 0) - (right[index] ?? 0);
    if (difference !== 0) return difference;
  }
  return 0;
}

function highest(versions) {
  return versions.length === 0
    ? null
    : [...versions].sort(compareVersions).at(-1);
}

// Returns the DT_NEEDED libraries, every versioned dependency from
// .gnu.version_r, and the highest GLIBC_x.y / GLIBCXX_x.y.z / CXXABI_x.y
// versions required.
export function readElfRequirements(bytes) {
  const { machine, sections } = readSections(bytes);
  const dynamic = sections.filter((section) => section.type === SHT_DYNAMIC);
  if (dynamic.length !== 1) fail("expected exactly one dynamic section");
  const dynstr = linkedStringTable(sections, dynamic[0], "dynamic section");
  const needed = [];
  for (
    let offset = dynamic[0].offset;
    offset + 16 <= dynamic[0].offset + dynamic[0].size;
    offset += 16
  ) {
    const tag = u64(bytes, offset, "d_tag");
    if (tag === DT_NULL) break;
    if (tag === DT_NEEDED)
      needed.push(
        cString(bytes, dynstr, u64(bytes, offset + 8, "d_val"), "DT_NEEDED"),
      );
  }

  const versions = {};
  const verneed = sections.filter(
    (section) => section.type === SHT_GNU_VERNEED,
  );
  if (verneed.length > 1) fail("multiple .gnu.version_r sections");
  for (const section of verneed) {
    const strings = linkedStringTable(sections, section, ".gnu.version_r");
    let offset = section.offset;
    for (let entry = 0; entry < section.info; entry += 1) {
      if (entry >= MAX_VERNEED_ENTRIES) fail("too many version needs");
      if (offset + 16 > section.offset + section.size)
        fail("version need outside .gnu.version_r");
      if (u16(bytes, offset, "vn_version") !== 1)
        fail("unsupported version need revision");
      const count = u16(bytes, offset + 2, "vn_cnt");
      const file = cString(
        bytes,
        strings,
        u32(bytes, offset + 4, "vn_file"),
        "vn_file",
      );
      if (!versions[file]) versions[file] = [];
      const list = versions[file];
      let aux = offset + u32(bytes, offset + 8, "vn_aux");
      for (let index = 0; index < count; index += 1) {
        if (aux + 16 > section.offset + section.size)
          fail("version need auxiliary entry outside .gnu.version_r");
        const name = cString(
          bytes,
          strings,
          u32(bytes, aux + 8, "vna_name"),
          "vna_name",
        );
        if (!list.includes(name)) list.push(name);
        const next = u32(bytes, aux + 12, "vna_next");
        if (index + 1 < count && next === 0)
          fail("truncated version need auxiliary list");
        aux += next;
      }
      list.sort();
      const next = u32(bytes, offset + 12, "vn_next");
      if (entry + 1 < section.info && next === 0)
        fail("truncated version need list");
      offset += next;
    }
  }
  const all = Object.values(versions).flat();
  const pick = (pattern) =>
    highest(
      all
        .map((name) => pattern.exec(name)?.[1])
        .filter((version) => version !== undefined),
    );
  return {
    machine,
    needed: [...needed].sort(),
    versions,
    minGlibc: pick(/^GLIBC_(\d+\.\d+(?:\.\d+)?)$/),
    minGlibcxx: pick(/^GLIBCXX_(\d+\.\d+(?:\.\d+)?)$/),
    minCxxabi: pick(/^CXXABI_(\d+\.\d+(?:\.\d+)?)$/),
  };
}
