import streamlit as st
import os
import sys

sys.path.append(os.path.join(os.getcwd(), 'src'))

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StructType, StructField
import altair as alt
from models import ALSRecommender, ContentBasedRecommender, HybridRecommender, ModelComparator
from utils.hbase_utils import get_all_data_from_hbase

# Page config
st.set_page_config(page_title="Movie Recommendation System", layout="wide")

@st.cache_resource
def get_spark_and_models():
    spark = SparkSession.builder \
        .appName("Movie_recommender_app") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    ratings, movies, tags = get_all_data_from_hbase(spark)
    ratings.cache()
    movies.cache()
    tags.cache()

    train_data, test_data = ratings.randomSplit([.8, .2], seed=42)
    
    comparator = ModelComparator(spark)
    
    # ALS
    als_model = ALSRecommender(spark)
    als_model.train(train_data, rank=15, maxIter=10, regParam=0.1)
    als_preds = als_model.predict(test_data)
    comparator.evaluate(als_preds, "ALS")
    
    cbf_model = ContentBasedRecommender(spark)
    cbf_model.train(movies, tags)
    cbf_preds = cbf_model.predict(test_data, train_data)
    comparator.evaluate(cbf_preds, "Content-Based")
    
    hybrid_model = HybridRecommender(spark, als_model, cbf_model, alpha=0.7, beta=0.3)
    hybrid_preds = hybrid_model.predict(test_data, train_data)
    comparator.evaluate(hybrid_preds, "Hybrid")
    
    return spark, ratings, movies, als_model, cbf_model, hybrid_model, comparator

with st.spinner("Initializing Spark and training models... this may take a minute"):
    spark, ratings, movies, als_model, cbf_model, hybrid_model, comparator = get_spark_and_models()

st.sidebar.title("🎬 Recommender")
page = st.sidebar.radio("Go to", ["Recommendations", "Model Comparison"])

if page == "Recommendations":
    st.title("🍿 Movie Recommendations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        user_id = st.number_input("Enter User ID", min_value=1, value=1, step=1)
        model_type = st.selectbox("Select Model", ["ALS", "Content-Based", "Hybrid"])
    
    with col2:
        movie_id = st.number_input("Enter Movie ID for similar movies/prediction", min_value=1, value=1, step=1)
        top_n = st.slider("Number of recommendations", 5, 20, 10)

    if st.button("Get Recommendations"):
        st.divider()
        st.subheader(f"Top {top_n} recommendations for User {user_id} ({model_type})")
        
        with st.spinner("Calculating..."):
            if model_type == "ALS":
                recs = als_model.recommend_for_user(user_id, top_n)
            elif model_type == "Content-Based":
                recs = cbf_model.recommend_for_user(user_id, ratings, top_n)
            else:
                recs = hybrid_model.recommend_for_user(user_id, ratings, movies, top_n)
            
            if model_type != "Hybrid":
                recs_df = recs.join(movies, "movieId").select("movieId", "title", "genres", col("score").alias("predicted_rating")).toPandas()
            else:
                recs_df = recs.select("movieId", "title", "genres", "score").toPandas()
                
            st.table(recs_df)

    st.divider()
    
    col_sim, col_pred = st.columns(2)
    
    with col_sim:
        st.subheader(f"Movies similar to Movie {movie_id}")
        if st.button("Find Similar"):
            with st.spinner("Finding similar movies..."):
                similar = cbf_model.find_similar_movies(movie_id, 5)
                if similar:
                    st.table(similar.toPandas())
                else:
                    st.error("Movie not found in content features")

    with col_pred:
        st.subheader(f"Predict Rating: User {user_id} → Movie {movie_id}")
        if st.button("Predict"):
            if movie_id > 2147483647 or user_id > 2147483647:
                st.error("ID too large. Maximum ID allowed is 2,147,483,647.")
            else:
                with st.spinner("Predicting..."):
                    try:
                        schema = StructType([
                            StructField("userId", IntegerType(), False),
                            StructField("movieId", IntegerType(), False)
                        ])
                        test_df = spark.createDataFrame([(int(user_id), int(movie_id))], schema)
                        
                        if model_type == "ALS": 
                            preds = als_model.predict(test_df)
                        elif model_type == "Content-Based": 
                            preds = cbf_model.predict(test_df, ratings)
                        else: 
                            preds = hybrid_model.predict(test_df, ratings)
                        
                        res = preds.collect()
                        if res and res[0].prediction is not None:
                            st.metric("Predicted Rating", f"{res[0].prediction:.2f} / 5.0")
                        else:
                            st.warning("Could not predict rating (Movie may not exist in features)")
                    except Exception as e:
                        st.error(f"Error during prediction: {str(e)}")

else: # Model Comparison
    st.title("📊 Model Comparison")
    st.write("Performance metrics evaluated on 20% test data split.")
    
    results = comparator.results
    if results:
        df_results = pd.DataFrame(results).T
        st.table(df_results)
        
        st.subheader("RMSE Comparison (Bar)")
        
        chart_data = df_results.reset_index().rename(columns={'index': 'Model'})
        
        color_scale = alt.Scale(
            domain=['ALS', 'Content-Based', 'Hybrid'],
            range=['#2ecc71', '#e74c3c', '#f1c40f']
        )
        
        bar_chart = alt.Chart(chart_data).mark_bar().encode(
            x=alt.X('Model:N', sort=None),
            y='RMSE:Q',
            color=alt.Color('Model:N', scale=color_scale),
            tooltip=['Model', 'RMSE', 'MAE']
        ).properties(height=400)
        
        st.altair_chart(bar_chart, use_container_width=True)

        # Line chart for overall performance comparison
        st.subheader("Metric Trends (Line)")
        st.line_chart(df_results)
    else:
        st.info("No evaluation results available.")

st.sidebar.divider()
st.sidebar.info("Big Data Project - IE212")
