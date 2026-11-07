use std::path::{Path, PathBuf};

use agent_knowledge::plug::{self, Agent};

fn env_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let pairs: Vec<(String, String)> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    move |key| pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

#[test]
fn embeds_every_skill_with_frontmatter() {
    let names: Vec<&str> = plug::SKILLS.iter().map(|s| s.name).collect();

    assert_eq!(names, ["knowledge-share", "knowledge-mount", "knowledge-write", "knowledge-read"]);
    for skill in plug::SKILLS {
        assert!(skill.body.starts_with(&format!("---\nname: {}\n", skill.name)), "{} frontmatter", skill.name);
    }
}

#[test]
fn skills_dir_defaults_under_home() {
    let env = env_from(&[("HOME", "/h")]);

    assert_eq!(plug::skills_dir(Agent::Claude, &env).unwrap(), PathBuf::from("/h/.claude/skills"));
    assert_eq!(plug::skills_dir(Agent::Codex, &env).unwrap(), PathBuf::from("/h/.codex/skills"));
}

#[test]
fn skills_dir_honors_agent_config_env() {
    let env = env_from(&[("HOME", "/h"), ("CLAUDE_CONFIG_DIR", "/c"), ("CODEX_HOME", "/x")]);

    assert_eq!(plug::skills_dir(Agent::Claude, &env).unwrap(), PathBuf::from("/c/skills"));
    assert_eq!(plug::skills_dir(Agent::Codex, &env).unwrap(), PathBuf::from("/x/skills"));
}

#[test]
fn skills_dir_fails_without_home() {
    let err = plug::skills_dir(Agent::Claude, &env_from(&[])).unwrap_err();

    assert!(err.to_string().contains("HOME"), "{err}");
}

#[test]
fn install_writes_every_skill_and_overwrites_old_copies() {
    let root = tempfile::tempdir().unwrap();
    let stale = root.path().join("knowledge-share/SKILL.md");
    std::fs::create_dir_all(stale.parent().unwrap()).unwrap();
    std::fs::write(&stale, "old").unwrap();

    let written = plug::install(root.path()).unwrap();

    assert_eq!(written.len(), plug::SKILLS.len());
    for skill in plug::SKILLS {
        let path: &Path = &root.path().join(skill.name).join("SKILL.md");
        assert_eq!(std::fs::read_to_string(path).unwrap(), skill.body);
    }
}
