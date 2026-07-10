---
title: Database Schema
description: Mô tả cấu trúc cơ sở dữ liệu và các quy tắc quản lý dữ liệu.
type: concept
---

# Database Schema

Tài liệu này lưu trữ thông tin về các bảng chính trong hệ thống và quy tắc thiết kế Database.

## 1. Naming Conventions (Quy tắc đặt tên)

- **Tên bảng:** Viết thường, sử dụng `snake_case` và ở dạng số nhiều (VD: `users`, `orders`, `product_categories`).
- **Tên cột:** Viết thường, `snake_case` (VD: `first_name`, `created_at`).
- **Khóa chính (Primary Key):** Luôn là `id` (kiểu UUID hoặc BigInt Auto Increment).
- **Khóa ngoại (Foreign Key):** Kết hợp tên bảng số ít và `id` (VD: `user_id`, `order_id`).

## 2. Cấu trúc các bảng chính

### Bảng `users`

Lưu trữ thông tin người dùng trong hệ thống.

| Cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :--- | :--- | :--- | :--- |
| `id` | UUID | PRIMARY KEY | ID định danh người dùng |
| `email` | VARCHAR(255) | UNIQUE, NOT NULL | Địa chỉ email đăng nhập |
| `password_hash` | VARCHAR(255) | NOT NULL | Mật khẩu đã mã hóa |
| `full_name` | VARCHAR(100) | | Họ tên đầy đủ |
| `status` | VARCHAR(20) | DEFAULT 'active' | Trạng thái (active, banned) |
| `created_at` | TIMESTAMP | DEFAULT NOW() | Thời gian tạo |
| `updated_at` | TIMESTAMP | DEFAULT NOW() | Thời gian cập nhật gần nhất |

### Bảng `roles` (Phân quyền)

| Cột | Kiểu dữ liệu | Ràng buộc | Mô tả |
| :--- | :--- | :--- | :--- |
| `id` | SERIAL | PRIMARY KEY | ID định danh |
| `name` | VARCHAR(50) | UNIQUE, NOT NULL | Tên quyền (VD: admin, user, editor) |
| `description`| TEXT | | Mô tả chi tiết quyền hạn |

## 3. Migration Flow

Khi cần thay đổi cấu trúc database, tuyệt đối không sửa trực tiếp. 
Mọi thay đổi phải thông qua hệ thống **Database Migration** (VD: Prisma, TypeORM, Flyway) để đảm bảo có thể rollback khi có sự cố và đồng bộ giữa các môi trường dev, staging và production.
