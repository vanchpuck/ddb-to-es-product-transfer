from sklearn.feature_extraction.text import CountVectorizer
from sklearn.neighbors import NearestNeighbors
from joblib import load
import logging
import boto3
import numpy as np
import json
import itertools
from decimal import Decimal


class BrandModelData:

    def __init__(self, brand_products, vectorizer, nn_model):
        self.brand_products = brand_products
        self.vectorizer = vectorizer
        self.nn_model = nn_model


class Classifier:

    path_prefix = "product-classifier"

    def __init__(self, model_bucket):
        self.model_bucket = model_bucket
        self.s3 = boto3.resource('s3')
        self.models = dict()

    def classify(self, brand, product_name) -> str:
        """Returns the original product name"""
        if brand not in self.models:
            self.models[brand] = self.read_brand_model_data(brand)
        model: BrandModelData = self.models[brand]
        doc_term_matrix = model.vectorizer.transform([product_name]).toarray()
        if not np.sum(doc_term_matrix) > 0:
            return product_name
        else:
            n_neighbour = model.nn_model.kneighbors(doc_term_matrix)
            if n_neighbour[0][0][0] == n_neighbour[0][0][2]:
                return product_name
            else:
                return model.brand_products[n_neighbour[1][0][0]]

    def __read_brand_model_data(self, brand):
        model = self.read_dump(self.model_bucket, "{}/{}/{}".format(self.path_prefix, brand, "model.joblib"))
        products = list(self.read_lines(self.model_bucket, "{}/{}/{}".format(self.path_prefix, brand, "products.txt")))
        vectorizer = CountVectorizer(ngram_range=(1, 2), binary=True)
        return BrandModelData(products, vectorizer, model)

    def __read_lines(self, bucket, key):
        obj = self.s3.Object(bucket, key)
        return map(lambda line: line.decode('utf-8'), obj.get()['Body'].iter_lines())

    def __read_dump(self, bucket, key):
        local_path = "/tmp/" + key.replace("/", "-")
        self.s3.Bucket(bucket).download_file(key, local_path)
        with open(local_path, 'rb') as file:
            return load(file)