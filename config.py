

# ---- CHANGE THESE ----
PROJECT_ID = "mj-gupta-514311"
REGION = "US"            # used later for Dataproc / Composer
BUCKET_NAME = "ecommerce-mj"  # used later for raw / silver layers
# ----------------------

ENTITIES = ["orders", "transactions", "products", "customers"]
 
TOPICS = {e: f"ecom-{e}-topic" for e in ENTITIES}
SUBSCRIPTIONS = {e: f"ecom-{e}-sub" for e in ENTITIES}
 
# Business key of each entity (used for the null-key test data and later for dedup)
KEY_FIELD = {
    "orders": "order_id",
    "transactions": "transaction_id",
    "products": "product_id",
    "customers": "customer_id",
}
 
# ---- Consumer settings ----
RAW_PREFIX = "raw"        # files go to gs://<bucket>/raw/<entity>/dt=<UTC ingestion date>/
PULL_BATCH_SIZE = 1000    # messages per pull = max messages per raw file
MAX_EMPTY_PULLS = 3       # stop an entity after this many consecutive empty pulls
 