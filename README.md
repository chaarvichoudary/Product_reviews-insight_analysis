# Product_reviews-insight_analysis

A real-time product review analysis system built using Apache Kafka, Apache HBase, and Machine Learning (SVM). This project processes product reviews, performs sentiment analysis, and stores the results in a distributed database for further analysis.



🚀 Features

Real-time processing of product reviews using Apache Kafka
Sentiment analysis using Support Vector Machine (SVM)
Distributed storage using Apache HBase
Containerized deployment using Docker
Scalable architecture for handling large volumes of review data



🛠️ Technology Stack

Apache Kafka: Message streaming and real-time data processing
Apache HBase: Distributed database storage
Python 3.x: Programming language
scikit-learn: Machine learning library for sentiment analysis
Docker: Containerization and deployment
Pandas: Data manipulation and analysis



📋 Prerequisites

Docker and Docker Compose
Python 3.x
Git



📈 Data Flow

Product reviews are read from the CSV file

Reviews are sent to Kafka topic 'product_reviews'

The main application consumes messages from Kafka

SVM model performs sentiment analysis on the reviews

Results are stored in HBase for persistence

Analysis results can be queried from HBase


👥 Authors

Rishabh Natarajan 

Chaarvi sai renuka choudary Ayineni



🙏 Acknowledgments

Apache Kafka documentation

Apache HBase documentation

scikit-learn documentation

Docker documentation
