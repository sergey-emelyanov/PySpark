### Pyspark

Я с pyspark до этого момента не сталкивался и для меня это первое знакомство. 

Как минимум можно уже выпустить отдельный гайд по установке pyspark, но об этом в другой раз. 

И так pyspark и все необходимые зависимости настроены и установлены, с чего начать? 

Все начинается с создание точки входа, а именно объекта SparkSession 
```python 
from pyspark.sql import SparkSession  
  
spark = SparkSession.builder.master('local[*]').appName('mindbox').getOrCreate()
```

***builder()*** - этот метод создает SparkSession
***master('local[*]')*** - говорит нам о том что мы работаем локально, на своей машине 
***appName('mindbox')*** - задаем имя нашему приложению 
***getOrCreate()*** - метод создает новый объект сессии если его нет, или вернет уже существующий

Следующая задача нам необходимо создать три DataFrame для продуктов, для категорий и для связи между ними. 

Для этого нам пригодиться метод createDataFrame, начнем с продуктов 
```python 
products = spark.createDataFrame([  
    {"productId": 1, "productName": "Product 1"},  
    {"productId": 2, "productName": "Product 2"},  
    {"productId": 3, "productName": "Product 3"},  
    {"productId": 4, "productName": "Product 4"},  
])  
products.show()
```

И с помощью метода show() убедимся, что все создалось как мы и планировали. 

```
+---------+-----------+
|productId|productName|
+---------+-----------+
|        1|  Product 1|
|        2|  Product 2|
|        3|  Product 3|
|        4|  Product 4|
+---------+-----------+
```

Ура! Маленькая победа, все работает именно так как мы и хотели.  Теперь создадим DataFrame для категорий их связи между продуктами и категориями. 

```
+----------+------------+
|categoryID|categoryName|
+----------+------------+
|         1|  Category A|
|         2|  Category B|
|         3|  Category C|
+----------+------------+
```

```
+----------+---------+
|categoryID|productId|
+----------+---------+
|         1|        1|
|         1|        2|
|         2|        3|
|         3|        3|
+----------+---------+
```

DataFrame связи продуктов и категорий я специально сделал так, что 4ый продукт остался без категории, а продукты 1 и 2 имеют одну и туже категорию.

Подготовительная работа окончена, переходим к основному заданию. Нам надо получить такой результирующий DataFrame, в котором каждому продукту соответствует его категория, если категории нет, то продукт все равно должен присутствовать в DataFrame. 

Когда у нас есть задача, которую с ходу не всегда ясно как решить имеет смысл разбить ее на подзадачи, можно начать с того чтобы для каждого продукта выявить ID категории, которая ему соответствует , тут нам на помощь придет LEFT JOIN, мы выбираем LEFT JOIN потому что нам важно, чтобы все продукты попали в результирующий DataFrame, даже если у них нет категории.

```python 
prod_join_prod_cat = products.join(prod_cat, products.productId == prod_cat.productId, "left").select('productName','categoryID')
```

Получим таблицу  вида 

```
+-----------+----------+
|productName|categoryID|
+-----------+----------+
|  Product 1|         1|
|  Product 2|         1|
|  Product 3|         3|
|  Product 3|         2|
|  Product 4|      NULL|
+-----------+----------+
```

Теперь так же используя LEFT JOIN получившуюся таблицу и таблицу с категориями 

```python
result = prod_join_prod_cat.join(categories, prod_join_prod_cat.categoryID == categories.categoryID, "left").select(  
    'productName', 'categoryName')
```

И получаем то, что мы и хотели 

```
+-----------+------------+
|productName|categoryName|
+-----------+------------+
|  Product 1|  Category A|
|  Product 2|  Category A|
|  Product 3|  Category C|
|  Product 3|  Category B|
|  Product 4|        NULL|
+-----------+------------+
```

Теперь когда "трудный" путь позади, он уже не кажется таким сложным и можно записать все одной строкой. 

```python
result = products.join(prod_cat, products.productId == prod_cat.productId, "left").join(  
        categories, prod_cat.categoryId == categories.categoryId, "left").select('productName', 'categoryName')
```

