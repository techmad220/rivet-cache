from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected one anchor in {path}, found {count}: {old!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# Group block coordinates into a value object instead of suppressing the
# too-many-arguments lint on the public constructor.
replace_once(
    "src/kv.rs",
    "#[derive(Debug, Clone, PartialEq, Eq, Hash)]\npub struct KvBlockKey {\n",
    "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]\npub struct KvBlockRange {\n    pub block_index: u32,\n    pub token_start: u32,\n    pub token_count: u32,\n    pub layer_start: u32,\n    pub layer_count: u32,\n    pub layout_version: u32,\n}\n\n#[derive(Debug, Clone, PartialEq, Eq, Hash)]\npub struct KvBlockKey {\n",
)
replace_once(
    "src/kv.rs",
    "    pub fn from_prefix(\n        model_fingerprint: impl Into<String>,\n        prefix_tokens: &[u32],\n        block_index: u32,\n        token_start: u32,\n        token_count: u32,\n        layer_start: u32,\n        layer_count: u32,\n        layout_version: u32,\n    ) -> Self {\n",
    "    pub fn from_prefix(\n        model_fingerprint: impl Into<String>,\n        prefix_tokens: &[u32],\n        range: KvBlockRange,\n    ) -> Self {\n",
)
replace_once(
    "src/kv.rs",
    "            block_index,\n            token_start,\n            token_count,\n            layer_start,\n            layer_count,\n            layout_version,\n",
    "            block_index: range.block_index,\n            token_start: range.token_start,\n            token_count: range.token_count,\n            layer_start: range.layer_start,\n            layer_count: range.layer_count,\n            layout_version: range.layout_version,\n",
)

replace_once(
    "src/kv.rs",
    "            keys.push(KvBlockKey::from_prefix(\n                self.model_fingerprint.clone(),\n                &self.tokens[..prefix_end],\n                block_index as u32,\n                token_start as u32,\n                chunk.len() as u32,\n                self.layer_start,\n                self.layer_count,\n                self.layout_version,\n            ));\n",
    "            keys.push(KvBlockKey::from_prefix(\n                self.model_fingerprint.clone(),\n                &self.tokens[..prefix_end],\n                KvBlockRange {\n                    block_index: block_index as u32,\n                    token_start: token_start as u32,\n                    token_count: chunk.len() as u32,\n                    layer_start: self.layer_start,\n                    layer_count: self.layer_count,\n                    layout_version: self.layout_version,\n                },\n            ));\n",
)
replace_once(
    "src/kv.rs",
    "        KvBlockKey::from_prefix(\"model-a\", tokens, 0, 0, tokens.len() as u32, 0, 32, 1)\n",
    "        KvBlockKey::from_prefix(\n            \"model-a\",\n            tokens,\n            KvBlockRange {\n                block_index: 0,\n                token_start: 0,\n                token_count: tokens.len() as u32,\n                layer_start: 0,\n                layer_count: 32,\n                layout_version: 1,\n            },\n        )\n",
)
replace_once(
    "src/kv.rs",
    "        let other_model = KvBlockKey::from_prefix(\"model-b\", &[1, 2, 3], 0, 0, 3, 0, 32, 1);\n",
    "        let other_model = KvBlockKey::from_prefix(\n            \"model-b\",\n            &[1, 2, 3],\n            KvBlockRange {\n                block_index: 0,\n                token_start: 0,\n                token_count: 3,\n                layer_start: 0,\n                layer_count: 32,\n                layout_version: 1,\n            },\n        );\n",
)

# Derive Default and make the default variant explicit.
replace_once(
    "src/kv.rs",
    "#[derive(Debug, Clone, Copy, PartialEq, Eq)]\npub enum KvWritePolicy {\n    Primary,\n    All,\n}\n\nimpl Default for KvWritePolicy {\n    fn default() -> Self {\n        Self::Primary\n    }\n}\n",
    "#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]\npub enum KvWritePolicy {\n    #[default]\n    Primary,\n    All,\n}\n",
)

# Public export and README example are produced by the main finisher first.
replace_once(
    "src/lib.rs",
    "    ContextCacheTier, CopyTransport, KvAllocator, KvBlock, KvBlockKey, KvCaptureRequest,\n",
    "    ContextCacheTier, CopyTransport, KvAllocator, KvBlock, KvBlockKey, KvBlockRange,\n    KvCaptureRequest,\n",
)
replace_once(
    "README.md",
    "    ContextCache, ContextCacheTier, KvBlock, KvBlockKey, KvEngine, KvWritePolicy,\n",
    "    ContextCache, ContextCacheTier, KvBlock, KvBlockKey, KvBlockRange, KvEngine,\n    KvWritePolicy,\n",
)
replace_once(
    "README.md",
    "let key = KvBlockKey::from_prefix(\"model-fingerprint\", &[10, 20, 30], 0, 0, 3, 0, 32, 1);\n",
    "let key = KvBlockKey::from_prefix(\n    \"model-fingerprint\",\n    &[10, 20, 30],\n    KvBlockRange {\n        block_index: 0,\n        token_start: 0,\n        token_count: 3,\n        layer_start: 0,\n        layer_count: 32,\n        layout_version: 1,\n    },\n);\n",
)

print("RIVET_KV_API_FIX=READY")
