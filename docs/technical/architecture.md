---
title: Architecture Overview
description: Tổng quan về kiến trúc hệ thống của dự án.
type: concept
---

# Architecture Overview

Tài liệu này mô tả tổng quan về kiến trúc của hệ thống, giúp các lập trình viên mới hiểu được cách các thành phần tương tác với nhau.

## 1. High-level Architecture

Hệ thống được thiết kế theo mô hình **Microservices** (hoặc Modular Monolith tùy thuộc vào quy mô dự án). 
Các thành phần chính bao gồm:

- **Frontend (Client):** Ứng dụng Web / Mobile giao tiếp với hệ thống qua RESTful API hoặc GraphQL.
- **API Gateway:** Điểm tiếp nhận tất cả các request từ Client, xử lý rate limiting, authentication và routing đến các service tương ứng.
- **Core Services:** Các dịch vụ xử lý nghiệp vụ chính (User Service, Product Service, Order Service...).
- **Database Layer:** Sử dụng PostgreSQL cho dữ liệu quan hệ và Redis cho Caching.

## 2. Công nghệ sử dụng (Tech Stack)

- **Ngôn ngữ chính:** JavaScript / TypeScript
- **Framework/Thư viện:** React (Frontend), Node.js / Express / NestJS (Backend)
- **Database:** PostgreSQL, Redis
- **Message Broker:** RabbitMQ / Kafka cho các xử lý bất đồng bộ

## 3. Deployment Flow

Mã nguồn được quản lý trên GitHub. Khi có PR được merge vào nhánh `main`, hệ thống CI/CD (GitHub Actions) sẽ tự động:
1. Chạy Unit Test và Linter.
2. Build Docker images.
3. Deploy tự động lên môi trường Staging/Production qua Kubernetes (hoặc Docker Swarm).
