use agent_knowledge::research::{self, Effort};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Serve one HTTP request with `status` and `body`; return the base URL and the raw request.
async fn serve_once(status: &'static str, body: &'static str) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = socket.read(&mut buf).await.unwrap();
            request.extend_from_slice(&buf[..n]);
            let text = String::from_utf8_lossy(&request);
            if let Some(head_end) = text.find("\r\n\r\n") {
                let length = text[..head_end]
                    .lines()
                    .find_map(|line| line.to_lowercase().strip_prefix("content-length: ").map(str::to_owned))
                    .and_then(|n| n.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if request.len() >= head_end + 4 + length || n == 0 {
                    break;
                }
            }
        }
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8_lossy(&request).into_owned()
    });
    (base, task)
}

#[tokio::test]
async fn saves_the_answer_with_sources_and_sends_key_and_effort() {
    let body = r#"{"output":{"content":"Async is fine.[1]","sources":[{"url":"https://a.dev","title":null},{"url":"https://b.dev","title":"B"}]}}"#;
    let (base, server) = serve_once("200 OK", body).await;
    let bank = tempfile::tempdir().unwrap();

    let path = research::run(bank.path(), &base, "secret", "  Rust Async ", Effort::Lite).await.unwrap();

    let request = server.await.unwrap();
    assert!(request.starts_with("POST /v1/research "), "{request}");
    assert!(request.to_lowercase().contains("x-api-key: secret"), "{request}");
    assert!(
        request.contains(r#""research_effort":"lite""#) && request.contains(r#""input":"Rust Async""#),
        "{request}"
    );
    assert_eq!(path, bank.path().join("research/rust-async/rust-async.md"));
    let concept = std::fs::read_to_string(path).unwrap();
    assert!(concept.contains("resource: \"https://a.dev\"\n    title: \"https://a.dev\""), "{concept}");
    assert!(concept.contains("[^s2]: [B](https://b.dev)"), "{concept}");
}

#[tokio::test]
async fn an_api_error_reports_the_status_and_writes_nothing() {
    let (base, _server) = serve_once("401 Unauthorized", r#"{"detail":"bad key"}"#).await;
    let bank = tempfile::tempdir().unwrap();

    let error = research::run(bank.path(), &base, "secret", "topic", Effort::Standard).await.unwrap_err();

    let message = format!("{error:#}");
    assert!(message.contains("401") && message.contains("bad key") && !message.contains("secret"), "{message}");
    assert!(!bank.path().join("research").exists());
}

#[tokio::test]
async fn a_topic_without_letters_or_digits_fails_before_any_request() {
    let bank = tempfile::tempdir().unwrap();

    let error = research::run(bank.path(), "http://127.0.0.1:1", "k", "?!", Effort::Lite).await.unwrap_err();

    assert!(error.to_string().contains("no letters or digits"), "{error}");
}
