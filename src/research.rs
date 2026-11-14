//! `research`: ask the you.com Research API about a topic and save the answer in the memory bank.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::okf::{self, Report, Source};

/// The env var that holds the you.com API key.
pub const KEY_VAR: &str = "AGENT_KNOWLEDGE_YOUCOM_KEY";
/// The you.com API origin. Tests point the client at a local server instead.
pub const API_BASE: &str = "https://api.you.com";
const RESEARCH_PATH: &str = "/v1/research";
/// Deeper efforts take minutes.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
/// The longest folder name a topic turns into.
const MAX_SLUG: usize = 64;

/// How hard the Research API works on the answer. `frontier` needs background requests, so it is
/// not offered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Effort {
    #[default]
    Lite,
    Standard,
    Deep,
    Exhaustive,
}

impl Effort {
    fn as_str(self) -> &'static str {
        match self {
            Effort::Lite => "lite",
            Effort::Standard => "standard",
            Effort::Deep => "deep",
            Effort::Exhaustive => "exhaustive",
        }
    }
}

/// The API key from `env`. Fails if it is unset or blank.
pub fn api_key(env: impl Fn(&str) -> Option<String>) -> Result<String> {
    match env(KEY_VAR).map(|key| key.trim().to_owned()) {
        Some(key) if !key.is_empty() => Ok(key),
        _ => bail!("{KEY_VAR} is not set; export your you.com API key in it"),
    }
}

#[derive(Deserialize)]
struct Response {
    output: Output,
}

#[derive(Deserialize)]
struct Output {
    content: String,
    #[serde(default)]
    sources: Vec<ApiSource>,
}

#[derive(Deserialize)]
struct ApiSource {
    url: String,
    title: Option<String>,
}

/// The answer from the Research API at `base`, as Markdown plus its sources.
pub async fn ask(base: &str, key: &str, topic: &str, effort: Effort) -> Result<(String, Vec<Source>)> {
    let client = reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let response = client
        .post(format!("{base}{RESEARCH_PATH}"))
        .header("X-API-Key", key)
        .json(&serde_json::json!({ "input": topic, "research_effort": effort.as_str() }))
        .send()
        .await
        .context("the you.com Research API is unreachable")?;
    let status = response.status();
    let body = response.text().await.context("cannot read the you.com response")?;
    if !status.is_success() {
        bail!("the you.com Research API answered {status}: {body}");
    }
    let parsed: Response = serde_json::from_str(&body).context("unexpected you.com response")?;
    let sources = parsed
        .output
        .sources
        .into_iter()
        .map(|source| Source {
            title: source.title.filter(|title| !title.trim().is_empty()).unwrap_or_else(|| source.url.clone()),
            url: source.url,
        })
        .collect();
    Ok((parsed.output.content, sources))
}

/// Research `topic` and save it under `<bank>/research/<slug>/`. Returns the concept's path.
pub async fn run(bank: &Path, base: &str, key: &str, topic: &str, effort: Effort) -> Result<PathBuf> {
    let topic = topic.trim();
    let Some(slug) = okf::slugify(topic, MAX_SLUG) else {
        bail!("the topic {topic:?} has no letters or digits to name its folder");
    };
    let (content, sources) = ask(base, key, topic, effort).await?;
    let at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let report = Report { topic: topic.to_owned(), slug, content, sources, at };
    okf::save(bank, &report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_with(value: Option<&'static str>) -> impl Fn(&str) -> Option<String> {
        move |key| (key == KEY_VAR).then_some(value).flatten().map(str::to_owned)
    }

    #[test]
    fn api_key_fails_when_unset_or_blank() {
        assert!(api_key(env_with(None)).unwrap_err().to_string().contains(KEY_VAR));
        assert!(api_key(env_with(Some("  "))).is_err());
        assert_eq!(api_key(env_with(Some(" k "))).unwrap(), "k");
    }

    #[test]
    fn effort_defaults_to_lite() {
        assert_eq!(Effort::default().as_str(), "lite");
    }
}
