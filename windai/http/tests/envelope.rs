use serde_json::json;
use wind_http::dto::envelope::ApiResponse;

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
