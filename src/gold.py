"""Gold layer — flatten nested business.json (attributes / hours / categories)."""

import ast

from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, BooleanType, MapType, StringType

from . import config

# ── UDFs ──────────────────────────────────────────────────────────────


def safe_parse_python_dict(s):
    """
    Parse string-encoded Python dict, vd "{'garage': False, 'street': True}".

    KHÔNG dùng json.loads — Yelp encode attributes bằng Python repr(), nên
    dùng True/False/None (Python) thay vì true/false/null (JSON chuẩn),
    json.loads sẽ raise lỗi parse.
    """
    if s is None or s == "None":
        return None
    try:
        return {str(k): str(v) for k, v in ast.literal_eval(s).items()}
    except Exception:
        return None


def parse_categories(s):
    """CSV string -> array<string>."""
    if s is None:
        return []
    return [c.strip() for c in s.split(",") if c.strip()]


def to_bool(s):
    """Normalize Yelp's Python-style boolean string ('True'/'False'/'None') -> BooleanType."""
    if s is None or s == "None":
        return None
    return s.strip().lower() == "true"


parse_dict_udf = udf(safe_parse_python_dict, MapType(StringType(), StringType()))
parse_categories_udf = udf(parse_categories, ArrayType(StringType()))
to_bool_udf = udf(to_bool, BooleanType())

# Sub-keys cần expand từ mỗi map field (string-encoded dict) thành column riêng
MAP_FIELD_KEYS = {
    "attr_parking_map": ("parking", ["garage", "street", "validated", "lot", "valet"]),
    "attr_ambience_map": (
        "ambience",
        ["touristy", "hipster", "romantic", "divey", "intimate", "trendy", "upscale", "classy", "casual"],
    ),
    "attr_good_for_meal_map": ("meal", ["dessert", "latenight", "lunch", "dinner", "brunch", "breakfast"]),
    "attr_music_map": ("music", ["dj", "background_music", "no_music", "jukebox", "live", "video", "karaoke"]),
    "attr_best_nights_map": (
        "best_night",
        ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
    ),
}


def expand_map_col(df, map_col: str, prefix: str, keys: list):
    """Extract từng key từ MapType column thành các column boolean riêng."""
    for key in keys:
        df = df.withColumn(f"{prefix}_{key}", to_bool_udf(col(map_col).getItem(key)))
    return df


def flatten_businesses(raw_df):
    """
    Flatten business.json: attributes (35 keys, một số là string-encoded
    dict cần parse 2 lớp), hours (7 keys -> 7 column), categories
    (CSV string -> array<string>). Trả về DataFrame phẳng, sẵn sàng ghi
    vào Gold.
    """
    flat_df = raw_df.select(
        col("business_id"),
        col("name"),
        col("address"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("latitude"),
        col("longitude"),
        col("stars"),
        col("review_count"),
        col("is_open"),
        parse_categories_udf(col("categories")).alias("categories"),
        to_bool_udf(col("attributes.RestaurantsTakeOut")).alias("attr_restaurants_takeout"),
        to_bool_udf(col("attributes.RestaurantsDelivery")).alias("attr_restaurants_delivery"),
        to_bool_udf(col("attributes.RestaurantsReservations")).alias("attr_restaurants_reservations"),
        to_bool_udf(col("attributes.OutdoorSeating")).alias("attr_outdoor_seating"),
        col("attributes.WiFi").alias("attr_wifi_raw"),  # 'free'/'paid'/'no' — categorical, KHÔNG phải boolean
        to_bool_udf(col("attributes.BikeParking")).alias("attr_bike_parking"),
        to_bool_udf(col("attributes.WheelchairAccessible")).alias("attr_wheelchair_accessible"),
        to_bool_udf(col("attributes.HappyHour")).alias("attr_happy_hour"),
        to_bool_udf(col("attributes.GoodForKids")).alias("attr_good_for_kids"),
        to_bool_udf(col("attributes.DogsAllowed")).alias("attr_dogs_allowed"),
        to_bool_udf(col("attributes.HasTV")).alias("attr_has_tv"),
        to_bool_udf(col("attributes.RestaurantsGoodForGroups")).alias("attr_good_for_groups"),
        to_bool_udf(col("attributes.Caters")).alias("attr_caters"),
        col("attributes.RestaurantsPriceRange2").alias("attr_price_range"),
        col("attributes.NoiseLevel").alias("attr_noise_level"),
        col("attributes.Alcohol").alias("attr_alcohol"),
        col("attributes.RestaurantsAttire").alias("attr_attire"),
        parse_dict_udf(col("attributes.BusinessParking")).alias("attr_parking_map"),
        parse_dict_udf(col("attributes.Ambience")).alias("attr_ambience_map"),
        parse_dict_udf(col("attributes.GoodForMeal")).alias("attr_good_for_meal_map"),
        parse_dict_udf(col("attributes.Music")).alias("attr_music_map"),
        parse_dict_udf(col("attributes.BestNights")).alias("attr_best_nights_map"),
        col("hours.Monday").alias("hours_monday"),
        col("hours.Tuesday").alias("hours_tuesday"),
        col("hours.Wednesday").alias("hours_wednesday"),
        col("hours.Thursday").alias("hours_thursday"),
        col("hours.Friday").alias("hours_friday"),
        col("hours.Saturday").alias("hours_saturday"),
        col("hours.Sunday").alias("hours_sunday"),
    )

    for map_col, (prefix, keys) in MAP_FIELD_KEYS.items():
        flat_df = expand_map_col(flat_df, map_col, prefix, keys)

    return flat_df.drop(*MAP_FIELD_KEYS.keys())


def write_gold_businesses(flat_df, spark) -> int:
    """Ghi batch (createOrReplace) vào Gold Iceberg table. Trả về số records."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.NAMESPACE_GOLD}")
    flat_df.writeTo(config.TABLE_GOLD_BUSINESSES).using("iceberg").tableProperty(
        "write.format.default", "parquet"
    ).createOrReplace()
    return spark.sql(f"SELECT COUNT(*) FROM {config.TABLE_GOLD_BUSINESSES}").collect()[0][0]


def build_and_write_gold(spark, business_path: str = None) -> int:
    """End-to-end: đọc business.json -> flatten -> ghi Gold. Trả về số records ghi được."""
    path = business_path or config.BUSINESS_JSON_PATH
    raw_df = spark.read.json(path)
    flat_df = flatten_businesses(raw_df)
    return write_gold_businesses(flat_df, spark)