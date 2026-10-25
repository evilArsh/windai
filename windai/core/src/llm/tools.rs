//! 工具的来源与清单

use super::ports::DeclaredTool;
use crate::agent::tool::tool_declarations;
use wind_wire::tool::Tools;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOrigin {
    Agent { name: String },
    Mcp { server: String, tool: String },
    ModRequest { mod_name: String, request: String },
}

impl ToolOrigin {
    pub fn exposed(&self) -> String {
        match self {
            ToolOrigin::Agent { name } => name.clone(),
            ToolOrigin::Mcp { server, tool } => format!("{server}__{tool}"),
            ToolOrigin::ModRequest { mod_name, request } => format!("m_{mod_name}__{request}"),
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        if let Some(rest) = name.strip_prefix("m_") {
            let (mod_name, request) = rest.split_once("__")?;
            return Some(ToolOrigin::ModRequest {
                mod_name: mod_name.to_string(),
                request: request.to_string(),
            });
        }
        match name.split_once("__") {
            Some((server, tool)) => Some(ToolOrigin::Mcp {
                server: server.to_string(),
                tool: tool.to_string(),
            }),
            None => Some(ToolOrigin::Agent {
                name: name.to_string(),
            }),
        }
    }
}

pub struct ToolCatalog {
    entries: Vec<(ToolOrigin, Tools)>,
}

impl ToolCatalog {
    pub fn assemble(from_mods: Vec<DeclaredTool>, from_mcp: Vec<DeclaredTool>) -> Self {
        let mut entries: Vec<(ToolOrigin, Tools)> = Vec::new();
        for tool in tool_declarations() {
            let name = match &tool {
                Tools::Function(function) => function.name.clone(),
            };
            entries.push((ToolOrigin::Agent { name }, tool));
        }
        for declared in from_mods.into_iter().chain(from_mcp) {
            entries.push((declared.origin, declared.declaration));
        }
        Self { entries }
    }

    pub fn declarations(&self) -> Vec<Tools> {
        self.entries.iter().map(|(_, tool)| tool.clone()).collect()
    }

    pub fn lookup(&self, name: &str) -> Option<&ToolOrigin> {
        self.entries
            .iter()
            .find(|(origin, _)| origin.exposed() == name)
            .map(|(origin, _)| origin)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wind_wire::tool::FunctionTool;

    #[test]
    fn the_three_origins_produce_disjoint_names() {
        let names = [
            ToolOrigin::Agent {
                name: "windai_spawn_agent".into(),
            }
            .exposed(),
            ToolOrigin::Mcp {
                server: "fs".into(),
                tool: "read".into(),
            }
            .exposed(),
            ToolOrigin::ModRequest {
                mod_name: "research".into(),
                request: "fetch".into(),
            }
            .exposed(),
        ];
        assert_eq!(names[0], "windai_spawn_agent");
        assert_eq!(names[1], "fs__read");
        assert_eq!(names[2], "m_research__fetch");
        let unique: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            3,
            "all three sources share one namespace and must not overlap"
        );
    }

    #[test]
    fn every_name_parses_back_to_its_origin() {
        for origin in [
            ToolOrigin::Agent {
                name: "windai_spawn_agent".into(),
            },
            ToolOrigin::Mcp {
                server: "fs".into(),
                tool: "read".into(),
            },
            ToolOrigin::ModRequest {
                mod_name: "research".into(),
                request: "fetch".into(),
            },
        ] {
            let exposed = origin.exposed();
            assert_eq!(ToolOrigin::parse(&exposed), Some(origin), "{exposed}");
        }
    }

    #[test]
    fn a_name_with_neither_prefix_is_an_agent_tool() {
        assert_eq!(
            ToolOrigin::parse("something_else"),
            Some(ToolOrigin::Agent {
                name: "something_else".into()
            })
        );
    }

    #[test]
    fn a_mod_name_that_starts_with_the_mcp_prefix_still_parses_as_a_mod() {
        // `m_` 与 `__` 两条判据的先后次序不能颠倒
        let origin = ToolOrigin::Mcp {
            server: "m_x".into(),
            tool: "y".into(),
        };
        let exposed = origin.exposed();
        assert_eq!(exposed, "m_x__y");
        assert_eq!(
            ToolOrigin::parse(&exposed),
            Some(ToolOrigin::ModRequest {
                mod_name: "x".into(),
                request: "y".into()
            }),
            "the rule is \"starting with `m_` means mod\", so this one parses as a mod"
        );
    }

    #[test]
    fn the_catalog_feeds_a_declaration_per_tool() {
        let catalog = ToolCatalog::assemble(Vec::new(), Vec::new());
        let declared = catalog.declarations();
        assert!(
            declared
                .iter()
                .any(|tool| matches!(tool, Tools::Function(f) if f.name == "windai_spawn_agent")),
            "the host's own agent tool is always in the catalog"
        );
        assert_eq!(
            catalog.len(),
            tool_declarations().len(),
            "the host's own tools enter the catalog only once"
        );
    }

    #[test]
    fn both_sources_land_in_the_catalog() {
        let from_mods = vec![declared(
            ToolOrigin::ModRequest {
                mod_name: "research".into(),
                request: "fetch".into(),
            },
            "m_research__fetch",
        )];
        let from_mcp = vec![declared(
            ToolOrigin::Mcp {
                server: "fs".into(),
                tool: "read".into(),
            },
            "fs__read",
        )];
        let catalog = ToolCatalog::assemble(from_mods, from_mcp);
        assert_eq!(
            catalog.lookup("m_research__fetch"),
            Some(&ToolOrigin::ModRequest {
                mod_name: "research".into(),
                request: "fetch".into(),
            })
        );
        assert_eq!(
            catalog.lookup("fs__read"),
            Some(&ToolOrigin::Mcp {
                server: "fs".into(),
                tool: "read".into(),
            })
        );
    }

    /// 构造一份工具声明
    fn declared(origin: ToolOrigin, exposed: &str) -> DeclaredTool {
        DeclaredTool {
            origin,
            declaration: Tools::Function(FunctionTool {
                name: exposed.to_string(),
                description: None,
                parameters: None,
                strict: None,
            }),
        }
    }
}
