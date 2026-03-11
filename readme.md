### Kiến trúc
![Kiến trúc](architecture.png)

### Các bước thực hiện
## 1. Clone repository về
## 2. Tạo file .env
1. Trong file env tạo `TMDB_READ_ACCESS_TOKEN =<token của TMDB_READ_ACCESS_TOKEN >` 
2. Tạo thêm `MINIO_ENDPOINT =<trong file docker-compose có>`
3. Tạo thêm `MINIO_ROOT_USER =<trong file docker-compose có>`
4. Tạo thêm `MINIO_ROOT_PASSWORD =<trong file docker-compose có>`
### 3. Lấy dữ liệu về
1. Chạy 2 file trong `ingestion`
   - `tmdb`
   - `run`
### 4. To be continue...