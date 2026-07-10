---
title: API Guidelines
description: Hướng dẫn chi tiết về thiết kế và sử dụng RESTful API.
type: concept
---

# API Guidelines

Quy chuẩn thiết kế API giúp đảm bảo tính nhất quán, dễ bảo trì và dễ sử dụng cho toàn bộ dự án.

## 1. Cấu trúc URL

Tất cả các API nên sử dụng danh từ số nhiều (plural nouns) và viết thường (kebab-case).

**Đúng:**
- `GET /api/v1/users`
- `POST /api/v1/user-profiles`
- `GET /api/v1/users/123/orders`

**Sai:**
- `GET /api/v1/getUsers`
- `POST /api/v1/createUser`

## 2. HTTP Methods

Sử dụng đúng HTTP methods cho từng mục đích:
- `GET`: Lấy dữ liệu (Không làm thay đổi trạng thái hệ thống).
- `POST`: Tạo mới tài nguyên.
- `PUT`: Cập nhật toàn bộ tài nguyên.
- `PATCH`: Cập nhật một phần tài nguyên.
- `DELETE`: Xóa tài nguyên.

## 3. HTTP Status Codes

Luôn trả về đúng mã lỗi để Client dễ dàng xử lý:
- `200 OK`: Thành công (GET, PUT, PATCH).
- `201 Created`: Tạo mới thành công (POST).
- `204 No Content`: Xóa thành công (DELETE).
- `400 Bad Request`: Dữ liệu gửi lên không hợp lệ.
- `401 Unauthorized`: Chưa xác thực (thiếu token hoặc token hết hạn).
- `403 Forbidden`: Đã xác thực nhưng không có quyền truy cập.
- `404 Not Found`: Không tìm thấy tài nguyên.
- `500 Internal Server Error`: Lỗi từ phía Server.

## 4. Định dạng Response

Tất cả API response nên tuân theo định dạng chuẩn JSON như sau:

```json
{
  "success": true,
  "data": {
    "id": 1,
    "name": "Nguyễn Văn A"
  },
  "message": "Lấy thông tin user thành công"
}
```
