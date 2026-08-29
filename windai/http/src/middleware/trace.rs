use tower_http::trace::{HttpMakeClassifier, TraceLayer};

pub fn trace_layer() -> TraceLayer<HttpMakeClassifier> {
    TraceLayer::new_for_http()
}
