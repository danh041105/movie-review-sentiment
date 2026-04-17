### Kiến trúc
![Kiến trúc](Architecture.png)

### Các bước thực hiện
## 1. Clone repository về
## 2. Tạo file .env
1. Trong file env tạo `TMDB_READ_ACCESS_TOKEN =<token của TMDB_READ_ACCESS_TOKEN >` 
2. Tạo thêm `MINIO_ENDPOINT =<trong file docker-compose có>`
3. Tạo thêm `MINIO_ROOT_USER =<trong file docker-compose có>`
4. Tạo thêm `MINIO_ROOT_PASSWORD =<trong file docker-compose có>`
5. Tạo thêm `TELEGRAM_BOT_TOKEN`
6. Tạo thêm `TELEGRAM_CHAT_ID`
7. Tạo thêm `REDIS_HOST`
8. Tạo thêm `REDIS_PORT`
9. Tạo thêm `POSTGRES_HOST`
10. Tạo thêm `POSTGRES_PORT`
11. Tạo thêm `POSTGRES_DB`
12. Tạo thêm `POSTGRES_USER`
13. Tạo thêm `POSTGRES_PASSWORD`
### Mở Airflow và bấm Trigger DAG movie_sentiment_full_pipeline