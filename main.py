from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[*]').appName('mindbox').getOrCreate()

products = spark.createDataFrame([
    {"productId": 1, "productName": "Product 1"},
    {"productId": 2, "productName": "Product 2"},
    {"productId": 3, "productName": "Product 3"},
    {"productId": 4, "productName": "Product 4"},
])

categories = spark.createDataFrame([
    {"categoryId": 1, "categoryName": "Category A"},
    {"categoryId": 2, "categoryName": "Category B"},
    {"categoryId": 3, "categoryName": "Category C"},
])

prod_cat = spark.createDataFrame([
    {"productId": 1, "categoryId": 1},
    {"productId": 2, "categoryId": 1},
    {"productId": 3, "categoryId": 2},
    {"productId": 3, "categoryId": 3},
])

# prod_join_prod_cat = products.join(prod_cat, products.productId == prod_cat.productId, "left").select(
#     'productName', 'categoryID')
#
# result = prod_join_prod_cat.join(categories, prod_join_prod_cat.categoryID == categories.categoryID, "left").select(
#     'productName', 'categoryName')

result = products.join(prod_cat, products.productId == prod_cat.productId, "left").join(
    categories, prod_cat.categoryId == categories.categoryId, "left").select('productName', 'categoryName')

result.show()
