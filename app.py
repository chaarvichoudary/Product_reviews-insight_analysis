from flask import Flask, render_template, request
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC
from kafka import KafkaProducer
import json
import os

# Kafka producer setup
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Error connecting to Kafka: {e}")

# Define file path
file_path = file_path = 'D:\\amazon_reviews\\amazon_reviews\\data\\modified_reviews.csv'


# Check if the file exists and load dataset
if os.path.exists(file_path):
    data = pd.read_csv(file_path).drop(
        ['Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12'], axis=1, errors='ignore'
    ).dropna(subset=['reviews_text']).reset_index(drop=True)
else:
    print(f"Dataset file not found at '{file_path}'. Please ensure the path is correct.")
    data = None

# Proceed only if data is loaded successfully
if data is not None:
    # Preprocessing function
    def preprocess_text(text):
        text = text.lower()
        return re.sub(r"[^a-zA-Z\s]", "", text)

    # Combine 'reviews_text' and 'reviews_title'
    data['combined_text'] = (data['reviews_title'].fillna('') + " " +
                             data['reviews_text'].fillna('')).apply(preprocess_text)

    # Sentiment labeling function
    def assign_sentiment(text):
        positive_keywords = ['good', 'excellent', 'fantastic', 'amazing', 'great']
        negative_keywords = ['bad', 'poor', 'terrible', 'waste', 'dissatisfied']
        if any(word in text for word in positive_keywords):
            return 1
        elif any(word in text for word in negative_keywords):
            return 0
        return 1

    data['sentiment'] = data['combined_text'].apply(assign_sentiment)

    # Train SVM model
    X = data['combined_text']
    y = data['sentiment']
    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(max_features=5000)),
        ('svm', LinearSVC())
    ])
    pipeline.fit(X, y)
else:
    print("Dataset is missing, exiting the script.")
    exit()  # Exit the script if the dataset is not loaded

# Flask app setup
app = Flask(__name__)

# Helper function to filter reviews by product_id
def filter_reviews_by_product_id(product_id, data):
    filtered_data = data[data['product_id'] == product_id]
    if filtered_data.empty:
        return None, f"No product found with ID: {product_id}"
    product_info = {
        "product_id": product_id,
        "product_name": filtered_data['product_name'].iloc[0],
        "product_brand": filtered_data['product_brand'].iloc[0],
        "product_categories": filtered_data['product_categories'].iloc[0],
        "product_manufacturer": filtered_data['product_manufacturer'].iloc[0]
    }
    return filtered_data, product_info

def analyze_product_reviews(product_id, data, model):
    filtered_reviews, product_info = filter_reviews_by_product_id(product_id, data)
    if filtered_reviews is None:
        return f"No product found with ID: {product_id}"

    review_texts = filtered_reviews['combined_text']
    predictions = model.predict(review_texts)

    total_reviews = len(predictions)
    positive_reviews = sum(predictions)
    negative_reviews = total_reviews - positive_reviews

    positive_percentage = (positive_reviews / total_reviews) * 100
    negative_percentage = (negative_reviews / total_reviews) * 100

    # Prepare analysis results
    analysis_results = {
        "product_id": product_id,
        "product_name": product_info["product_name"],
        "product_brand": product_info["product_brand"],
        "product_categories": product_info["product_categories"],
        "product_manufacturer": product_info["product_manufacturer"],
        "total_reviews": total_reviews,
        "positive_percentage": float(f"{positive_percentage:.2f}"),
        "negative_percentage": float(f"{negative_percentage:.2f}")
    }

    # Send to Kafka
    try:
        producer.send('product_analysis', value=analysis_results)
        producer.flush()
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

    # Format display output
    product_info_display = "\n".join([f"{key}: {value}" for key, value in product_info.items()])
    result_display = (
        f"Total Reviews: {total_reviews}\n"
        f"Positive Reviews: {positive_percentage:.2f}%\n"
        f"Negative Reviews: {negative_percentage:.2f}%"
    )
    return f"{product_info_display}\n\n{result_display}"

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    product_ids = data['product_id'].unique() if data is not None else []
    result = None

    if request.method == "POST" and data is not None:
        selected_product_id = request.form["product_id"]
        result = analyze_product_reviews(selected_product_id, data, pipeline)

    return render_template("index.html", product_ids=product_ids, result=result)

# Run the app
if __name__ == "__main__":
    app.run(debug=True)
