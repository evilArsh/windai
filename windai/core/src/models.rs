macro_rules! impl_display {
    ($ty:ident { $($field:ident),+ $(,)? }) => {
        #[allow(unused_assignments)]
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($ty), " {{ "))?;
                let mut first = true;
                $(
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, concat!(stringify!($field), ": {:?}"), &self.$field)?;
                )+
                write!(f, " }}")
            }
        }
    };
}

pub mod agent;
pub mod config;
pub mod conversation;
pub mod module;

pub use agent::*;
pub use config::*;
pub use conversation::*;
pub use module::*;
