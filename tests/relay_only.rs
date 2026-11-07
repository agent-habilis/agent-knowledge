//! Its own test binary, because `block_ip_paths` blocks direct paths for the whole process.
//! It needs internet access: the relay is the public one.

mod common;

use common::{Pair, wait_for};
use habilis_network::net::direct::block_ip_paths;

/// Two peers on networks where the hole punch fails have only the relay between them.
#[tokio::test(flavor = "multi_thread")]
async fn syncs_over_relay_only() {
    block_ip_paths(true);
    let share_dir = tempfile::tempdir().unwrap();
    let mount_dir = tempfile::tempdir().unwrap();
    std::fs::write(share_dir.path().join("start.md"), "from share").unwrap();

    let pair = Pair::start(share_dir.path(), mount_dir.path()).await;

    wait_for(&mount_dir.path().join("start.md"), Some("from share")).await;
    std::fs::write(mount_dir.path().join("back.md"), "from mount").unwrap();
    wait_for(&share_dir.path().join("back.md"), Some("from mount")).await;

    pair.stop().await;
}
