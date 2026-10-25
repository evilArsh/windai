//! 各 mod 声明的静态提示词片段

use super::manifest::Manifest;

pub fn declared_fragments(manifests: &[Manifest]) -> Vec<String> {
    let mut ordered: Vec<&Manifest> = manifests.iter().collect();
    ordered.sort_by(|left, right| left.name.cmp(&right.name));
    let mut out = Vec::new();
    for manifest in ordered {
        for prompt in manifest.prompts.values() {
            out.push(prompt.content.clone());
        }
    }
    out
}
