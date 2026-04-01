import pandas as pd
import torch
import re
from transformers import AutoTokenizer, AutoModelForSequenceClassification
class MovieRatingPredictor:
    _instance = None
    @classmethod
    def get_instance(cls):
        # Đảm bảo model chỉ được load 1 lần duy nhất trên mỗi Spark Worker
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        # Đường dẫn tới thư mục lưu model ở Bước 9
        self.model_path = "../models/bert-movie-rating-regressor"
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path).to(self.device)
        self.model.eval() # Chế độ dự báo

    def clean_text(self, text):
        if not text or pd.isna(text): return ""
        text = str(text).lower()
        text = re.sub(r'<.*?>', '', text)
        text = re.sub(r'[^a-z0-9.,!?\s]', '', text)
        return re.sub(r'\s+', ' ', text).strip()

    def predict(self, texts, batch_size=32):
        results = []
        # Xử lý theo từng mini-batch để không làm tràn RAM GPU
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i : i + batch_size]
            cleaned_texts = [self.clean_text(t) for t in batch_texts]
            
            inputs = self.tokenizer(
                cleaned_texts, 
                return_tensors="pt", 
                truncation=True, 
                padding=True, 
                max_length=256
            ).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            # Kéo giãn kết quả [0, 1] về lại thang [1, 10]
            preds = outputs.logits.squeeze(-1).cpu().numpy()
            ratings = (preds * 9.0) + 1.0
            
            # Cắt gọt để đảm bảo điểm luôn từ 1.0 đến 10.0
            results.extend([max(1.0, min(10.0, float(r))) for r in ratings])
            
        return results

# Hàm trung gian (UDF) để giao tiếp với Spark
def predict_rating_udf(text_series: pd.Series) -> pd.Series:
    predictor = MovieRatingPredictor.get_instance()
    predictions = predictor.predict(text_series.tolist())
    return pd.Series(predictions)