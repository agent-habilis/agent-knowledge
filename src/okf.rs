//! Open Knowledge Format (OKF v0.2) files for a research report: the concept, the indexes, the log.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// The bank folder that holds one subfolder per research topic.
pub const RESEARCH_DIR: &str = "research";
const ROOT_INDEX_HEAD: &str = "---\nokf_version: \"0.2\"\n---\n\n";
const RESEARCH_GROUP_LINE: &str =
    "* [Research](research/) - Research reports on a topic, from the you.com Research API.";
const LOG_TITLE: &str = "# Directory Update Log";
const GENERATED_BY: &str = "agent-knowledge/you-com-research";

/// One web source the report cites.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub title: String,
    pub url: String,
}

/// A finished research report, ready to render.
#[derive(Clone, Debug)]
pub struct Report {
    /// The topic as the user typed it.
    pub topic: String,
    /// Lowercase `[a-z0-9-]` name of the folder and the concept file.
    pub slug: String,
    /// The answer, in Markdown.
    pub content: String,
    pub sources: Vec<Source>,
    /// UTC time, `YYYY-MM-DDTHH:MM:SSZ`.
    pub at: String,
}

impl Report {
    fn description(&self) -> String {
        format!("Research report on {}, from the you.com Research API.", self.topic)
    }

    /// The concept path, relative to the bank.
    pub fn concept_path(&self) -> String {
        format!("{RESEARCH_DIR}/{0}/{0}.md", self.slug)
    }
}

/// `topic` as a folder name: lowercase ASCII letters and digits, every other run turned into one
/// `-`, no `-` at either end, at most `max` bytes. `None` if nothing is left.
pub fn slugify(topic: &str, max: usize) -> Option<String> {
    let mut slug = String::new();
    for c in topic.to_lowercase().chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.truncate(max);
    let slug = slug.trim_end_matches('-').to_owned();
    (!slug.is_empty()).then_some(slug)
}

/// A YAML double-quoted scalar. JSON strings are valid YAML double-quoted strings.
fn quoted(text: &str) -> String {
    serde_json::Value::String(text.to_owned()).to_string()
}

/// The first paragraph of `content` that is not a heading, or the whole content.
fn summary(content: &str) -> String {
    content
        .split("\n\n")
        .map(str::trim)
        .find(|block| !block.is_empty() && !block.starts_with('#'))
        .unwrap_or(content.trim())
        .to_owned()
}

/// The concept file: OKF frontmatter, `# Summary`, `# Details`, one footnote per source.
pub fn render(report: &Report) -> String {
    let mut out = String::from("---\ntype: Reference\n");
    out += &format!("title: {}\n", quoted(&report.topic));
    out += &format!("description: {}\n", quoted(&report.description()));
    out += "tags: [research]\n";
    out += &format!("generated: {{ by: {GENERATED_BY}, at: \"{}\" }}\nstatus: draft\n", report.at);
    if !report.sources.is_empty() {
        out += "sources:\n";
        for (n, source) in report.sources.iter().enumerate() {
            let id = n + 1;
            out += &format!(
                "  - id: s{id}\n    resource: {}\n    title: {}\n",
                quoted(&source.url),
                quoted(&source.title)
            );
        }
    }
    out += &format!("---\n\n# Summary\n\n{}\n\n# Details\n\n{}\n", summary(&report.content), report.content.trim());
    if !report.sources.is_empty() {
        out += "\n";
        for (n, source) in report.sources.iter().enumerate() {
            out += &format!("[^s{}]: [{}]({})\n", n + 1, source.title, source.url);
        }
    }
    out
}

/// Append `line` to `text` unless a line already contains `marker`.
fn with_line(text: &str, marker: &str, line: &str) -> String {
    if text.lines().any(|existing| existing.contains(marker)) {
        return text.to_owned();
    }
    let mut out = text.to_owned();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out + line + "\n"
}

/// `log` with `entry` under today's `## <date>` heading, newest first.
fn with_log_entry(log: &str, date: &str, entry: &str) -> String {
    let log = if log.trim().is_empty() { format!("{LOG_TITLE}\n") } else { log.to_owned() };
    let heading = format!("## {date}");
    let mut lines: Vec<String> = log.lines().map(str::to_owned).collect();
    match lines.iter().position(|line| line == &heading) {
        Some(at) => lines.insert(at + 1, entry.to_owned()),
        None => {
            let title = lines.iter().position(|line| line.starts_with("# ")).map_or(0, |at| at + 1);
            lines.splice(title..title, ["".to_owned(), heading, entry.to_owned()]);
        }
    }
    lines.join("\n") + "\n"
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn write(path: &Path, text: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("cannot create {}", parent.display()))?;
    }
    std::fs::write(path, text).with_context(|| format!("cannot write {}", path.display()))
}

/// Write `report` into `bank`, then update the research index, the root index and the log.
/// Returns the concept's full path.
pub fn save(bank: &Path, report: &Report) -> Result<PathBuf> {
    let concept = bank.join(report.concept_path());
    let action = if concept.exists() { "Update" } else { "Creation" };
    write(&concept, &render(report))?;

    let link = format!("({0}/{0}.md)", report.slug);
    let entry = format!("* [{}]{link} - {}", report.topic, report.description());
    let group_index = bank.join(RESEARCH_DIR).join("index.md");
    write(&group_index, &with_line(&read(&group_index), &link, &entry))?;

    let root_index = bank.join("index.md");
    let root = read(&root_index);
    let root = if root.trim().is_empty() { ROOT_INDEX_HEAD.to_owned() } else { root };
    write(&root_index, &with_line(&root, "(research/)", RESEARCH_GROUP_LINE))?;

    let log_path = bank.join("log.md");
    let date = &report.at[..10];
    let verb = if action == "Creation" { "Added" } else { "Refreshed" };
    let log_line = format!("* **{action}**: {verb} research [{}](/{}).", report.topic, report.concept_path());
    write(&log_path, &with_log_entry(&read(&log_path), date, &log_line))?;
    Ok(concept)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Report {
        Report {
            topic: "Rust: async?".to_owned(),
            slug: "rust-async".to_owned(),
            content: "# Answer\n\nRust async is great.[1]\n\nMore detail.".to_owned(),
            sources: vec![Source { title: "Tokio \"docs\"".to_owned(), url: "https://tokio.rs".to_owned() }],
            at: "2026-11-14T10:00:00Z".to_owned(),
        }
    }

    #[test]
    fn slugify_keeps_lowercase_letters_digits_and_single_dashes() {
        assert_eq!(slugify("Rust  Async: 2026!", 64).as_deref(), Some("rust-async-2026"));
        assert_eq!(slugify("--Café über--", 64).as_deref(), Some("caf-ber"));
        assert_eq!(slugify("abc def", 4).as_deref(), Some("abc"));
        assert_eq!(slugify("¿?!", 64), None);
        assert_eq!(slugify("", 64), None);
    }

    #[test]
    fn render_writes_quoted_frontmatter_summary_and_footnotes() {
        let text = render(&report());

        assert!(text.starts_with("---\ntype: Reference\ntitle: \"Rust: async?\"\n"), "{text}");
        assert!(text.contains("description: \"Research report on Rust: async?"), "{text}");
        assert!(text.contains("at: \"2026-11-14T10:00:00Z\""), "{text}");
        assert!(text.contains("  - id: s1\n    resource: \"https://tokio.rs\"\n    title: \"Tokio \\\"docs\\\"\"\n"));
        assert!(text.contains("# Summary\n\nRust async is great.[1]\n\n# Details\n\n# Answer"), "{text}");
        assert!(text.ends_with("[^s1]: [Tokio \"docs\"](https://tokio.rs)\n"), "{text}");
    }

    #[test]
    fn render_omits_sources_when_there_are_none() {
        let text = render(&Report { sources: vec![], ..report() });

        assert!(!text.contains("sources:"));
        assert!(!text.contains("[^s"));
    }

    #[test]
    fn log_entry_goes_under_todays_heading_newest_first() {
        let empty = with_log_entry("", "2026-11-14", "* a");
        assert_eq!(empty, "# Directory Update Log\n\n## 2026-11-14\n* a\n");

        let older = "# Directory Update Log\n\n## 2026-11-07\n* old\n";
        assert_eq!(
            with_log_entry(older, "2026-11-14", "* a"),
            "# Directory Update Log\n\n## 2026-11-14\n* a\n\n## 2026-11-07\n* old\n"
        );
        assert_eq!(with_log_entry(&empty, "2026-11-14", "* b"), "# Directory Update Log\n\n## 2026-11-14\n* b\n* a\n");
    }

    #[test]
    fn save_creates_concept_indexes_and_log_then_updates() {
        let bank = tempfile::tempdir().unwrap();

        let path = save(bank.path(), &report()).unwrap();
        save(bank.path(), &report()).unwrap();

        assert_eq!(path, bank.path().join("research/rust-async/rust-async.md"));
        let group = read(&bank.path().join("research/index.md"));
        assert_eq!(group.matches("(rust-async/rust-async.md)").count(), 1, "{group}");
        let root = read(&bank.path().join("index.md"));
        assert!(root.starts_with(ROOT_INDEX_HEAD) && root.matches("(research/)").count() == 1, "{root}");
        let log = read(&bank.path().join("log.md"));
        assert!(log.contains("## 2026-11-14\n* **Update**: Refreshed research"), "{log}");
        assert!(
            log.contains("* **Creation**: Added research [Rust: async?](/research/rust-async/rust-async.md)."),
            "{log}"
        );
    }
}
