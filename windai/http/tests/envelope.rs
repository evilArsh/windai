use serde_json::json;
use wind_core::error::CoreError;
use wind_http::dto::envelope::{ApiResponse, map_core_error};

#[test]
fn ok_serializes_data() {
    let r: ApiResponse<u32> = ApiResponse::ok(7);
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"code": 200, "data": 7, "msg": "ok"})
    );
}

#[test]
fn not_found_serializes_null_data() {
    let r: ApiResponse<u32> = ApiResponse::not_found("missing");
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"code": 404, "data": null, "msg": "missing"})
    );
}

#[test]
fn internal_serializes_null_data() {
    let r: ApiResponse<u32> = ApiResponse::internal("boom");
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"code": 500, "data": null, "msg": "boom"})
    );
}

#[test]
fn bad_request_serializes_code_400() {
    let r: ApiResponse<u32> = ApiResponse::bad_request("bad input");
    assert_eq!(
        serde_json::to_value(&r).unwrap(),
        json!({"code": 400, "data": null, "msg": "bad input"})
    );
}

#[test]
fn map_core_error_validation_maps_to_400() {
    let r: ApiResponse<u32> = map_core_error(CoreError::Validation("bad".into()));
    assert_eq!(r.code, 400);
    assert_eq!(r.msg, "bad");
}
