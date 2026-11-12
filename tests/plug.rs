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

    assert_eq!(plug::skills_dir(Agent::ClaudeCode, &env).unwrap(), PathBuf::from("/h/.claude/skills"));
    assert_eq!(plug::skills_dir(Agent::Codex, &env).unwrap(), PathBuf::from("/h/.codex/skills"));
    assert_eq!(plug::skills_dir(Agent::Pi, &env).unwrap(), PathBuf::from("/h/.pi/agent/skills"));
}

#[test]
fn skills_dir_honors_agent_config_env() {
    let env =
        env_from(&[("HOME", "/h"), ("CLAUDE_CONFIG_DIR", "/c"), ("CODEX_HOME", "/x"), ("PI_CODING_AGENT_DIR", "/p")]);

    assert_eq!(plug::skills_dir(Agent::ClaudeCode, &env).unwrap(), PathBuf::from("/c/skills"));
    assert_eq!(plug::skills_dir(Agent::Codex, &env).unwrap(), PathBuf::from("/x/skills"));
    assert_eq!(plug::skills_dir(Agent::Pi, &env).unwrap(), PathBuf::from("/p/skills"));
}

#[test]
fn skills_dir_fails_without_home() {
    let err = plug::skills_dir(Agent::ClaudeCode, env_from(&[])).unwrap_err();

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

#[test]
fn selects_only_detected_agents_by_default() {
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".pi/agent")).unwrap();
    let pairs = [("HOME", home.path().to_str().unwrap())];
    let env = env_from(&pairs);

    let selected = plug::selected(&[], |agent| plug::detected(agent, &env));

    assert_eq!(selected, [Agent::Pi]);
}

#[test]
fn explicit_agent_wins_over_detection() {
    let selected = plug::selected(&[Agent::Codex, Agent::Codex], |_| false);

    assert_eq!(selected, [Agent::Codex]);
}

#[test]
fn skills_name_every_agent_command_form() {
    for skill in plug::SKILLS {
        for form in ["/knowledge-mount", "$knowledge-mount", "/skill:knowledge-mount"] {
            assert!(skill.body.contains(form), "{} lacks {form}", skill.name);
        }
    }
}

/// A time in a YAML flow map holds `:`, and strict parsers (Ruby's Psych) reject it unquoted.
#[test]
fn knowledge_write_template_quotes_the_time() {
    let write = plug::SKILLS.iter().find(|s| s.name == "knowledge-write").unwrap();
    let line = write.body.lines().find(|l| l.starts_with("generated: {")).expect("a generated line");

    assert!(line.contains("at: \""), "unquoted time in {line:?}");
}
