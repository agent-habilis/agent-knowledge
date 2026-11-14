import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { updateLicense } from "./update-license";

const fixture = [
  "Copyright (C) 2026 Caio Gondim",
  "",
  "Licensor: Caio Gondim",
  "",
  "Licensed Work: agent-knowledge v0.1.0",
  "",
  "Change Date: November 13, 2030",
  "",
  "Change License: MIT License",
  "",
].join("\n");

test("sets the version and a Change Date 4 years after the release", () => {
  const out = updateLicense(fixture, "0.2.0", new Date("2027-03-09T12:00:00Z"));
  expect(out).toContain("Licensed Work: agent-knowledge v0.2.0\n");
  expect(out).toContain("Change Date: March 9, 2031\n");
});

test("changes only those two lines", () => {
  const out = updateLicense(fixture, "0.2.0", new Date("2027-03-09T12:00:00Z"));
  const before = fixture.split("\n");
  const after = out.split("\n");
  expect(after.length).toBe(before.length);
  const changed = before.filter((line, i) => line !== after[i]);
  expect(changed).toEqual([
    "Licensed Work: agent-knowledge v0.1.0",
    "Change Date: November 13, 2030",
  ]);
});

test("keeps the copyright year", () => {
  const out = updateLicense(fixture, "0.2.0", new Date("2029-01-01T00:00:00Z"));
  expect(out).toContain("Copyright (C) 2026 Caio Gondim");
});

test("uses the last day of February when the day does not exist", () => {
  const out = updateLicense(fixture, "1.0.0", new Date("2096-02-29T00:00:00Z"));
  expect(out).toContain("Change Date: February 28, 2100\n");
});

test("fails if a header line is missing", () => {
  expect(() => updateLicense("no header", "0.2.0", new Date())).toThrow();
});

test("the real LICENSE still has both lines the script rewrites", () => {
  const license = readFileSync(new URL("../LICENSE", import.meta.url), "utf8");
  expect(() => updateLicense(license, "9.9.9", new Date())).not.toThrow();
});
