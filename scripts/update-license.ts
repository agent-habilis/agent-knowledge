#!/usr/bin/env bun
import { readFileSync, writeFileSync } from "node:fs";

const MONTHS = [
  "January", "February", "March", "April", "May", "June", "July",
  "August", "September", "October", "November", "December",
];
const CHANGE_DATE_YEARS = 4;

function changeDate(now: Date): string {
  const year = now.getUTCFullYear() + CHANGE_DATE_YEARS;
  const month = now.getUTCMonth();
  const lastDay = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  const day = Math.min(now.getUTCDate(), lastDay);
  return `${MONTHS[month]} ${day}, ${year}`;
}

function replaceLine(text: string, prefix: string, value: string): string {
  const line = new RegExp(`^${prefix}.*$`, "m");
  if (!line.test(text)) throw new Error(`LICENSE has no "${prefix}" line`);
  return text.replace(line, `${prefix}${value}`);
}

export function updateLicense(text: string, version: string, now: Date): string {
  const withVersion = replaceLine(text, "Licensed Work: ", `agent-knowledge v${version}`);
  return replaceLine(withVersion, "Change Date: ", changeDate(now));
}

if (import.meta.main) {
  const version = process.argv[2];
  if (!version) throw new Error("usage: update-license.ts <version> [path]");
  const path = process.argv[3] ?? "LICENSE";
  writeFileSync(path, updateLicense(readFileSync(path, "utf8"), version, new Date()));
}
