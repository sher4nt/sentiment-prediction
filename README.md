# Описание задачи
Требуется предсказать настроение обзора товара. Использован планировщик Airflow для реализации DAG из следующих задач:
- предобработка тренировочного датасета на Spark
- обучение модели sklearn на этих данных 
- предобработка тестового датасета на Spark
- предсказание на тестовом датасете с помощью Spark и pandas_udf, используя предобученную модель sklearn

# Описание датасета
Датасет состоит из json-строк следующего вида:
``` 
{
    “id”:1, 
    "label": 1,
    "vote": "25",
    "verified": false,
    "reviewTime": "12 19, 2008",
    "reviewerID": "APV13CM0919JD",
    "asin": "B001GXRQW0",
    "reviewerName": "LEH",
    "reviewText": "Amazon,\nI am shopping for Amazon.com gift cards for Christmas gifts and am really so disappointed that out of five choices there isn't one that says \"Merry Christmas\" or mentions Christmas at all!  I am sure I am not alone in wanting a card that reflects the actual \"holiday\" we are celebrating. On principle, I cannot send a Amazon gift card this Christmas.  What's up with all the Political Correctness?  Bad marketing decision.\nLynn",
    "summary": "Merry Christmas.",
    "unixReviewTime": 1229644800
}

```
где 
- `id` -идентификатор записи
- `label` - настроение обзора со значениями 1 (позитивный обзор, соответствует рейтингам 4 и 5), и 0 (негативный обзор, соответствует рейтингам 1-3 включительно)
- `vote` - скольким людям понравился обзор
- `verified` - `True` если известно, что автор обзора купил этот товар на сайте
- `reviewTime` - время написания или опубликования обзора
- `reviewerID` - идентификатор автора
- `asin` - идентификатор товара
- `reviewerName` - имя автора
- `reviewText` - собственно текст обзора
- `summary` - короткое резюме по товару, или заголовок обзора
- `unixReviewTime` - время обзора в секундах эпохи
