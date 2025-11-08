from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
from faker import Faker
import pandas as pd
import matplotlib.pyplot as plt


OUTPUT_DIR = "/opt/airflow/data"
INTERMEDIATE_DIR = os.path.join(OUTPUT_DIR, "intermediate_data")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")

default_args = {"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",  # "00 22 * * *",
    catchup=False,
) as dag:

    @task()
    def fetch_categories(output_dir: str = OUTPUT_DIR) -> str:
        """Verify category.csv (data on product categories) exists in the data folder and returns either its file path to be used in future tasks if it exists or a FileNotFoundError if it does not exist."""
        category_file_path = os.path.join(output_dir, "category.csv")

        if not os.path.exists(category_file_path):
            raise FileNotFoundError(f"categories.csv not found at {category_file_path}")

        print(f"categories.csv found at {category_file_path}")
        return category_file_path

    @task()
    def fetch_sales(output_dir: str = OUTPUT_DIR) -> str:
        """Verify sales.csv (data on product sales) exists in the data folder and returns either its file path to be used in future tasks if it exists or a FileNotFoundError if it does not exist."""
        sales_file_path = os.path.join(output_dir, "sales.csv")

        if not os.path.exists(sales_file_path):
            raise FileNotFoundError(f"sales.csv not found at {sales_file_path}")

        print(f"sales.csv found at {sales_file_path}")
        return sales_file_path

    @task()
    def fetch_products(output_dir: str = OUTPUT_DIR) -> str:
        """Verify products.csv (general data on apple products) exists in the data folder and returns either its file path to be used in future tasks if it exists or a FileNotFoundError if it does not exist."""
        product_file_path = os.path.join(output_dir, "products.csv")

        if not os.path.exists(product_file_path):
            raise FileNotFoundError(f"products.csv not found at {product_file_path}")

        print(f"products.csv found at {product_file_path}")
        return product_file_path

    @task()
    def fetch_stores(output_dir: str = OUTPUT_DIR) -> str:
        """Verify stores.csv (data on apple stores) exists in the data folder and returns either its file path to be used in future tasks if it exists or a FileNotFoundError if it does not exist."""
        stores_file_path = os.path.join(output_dir, "stores.csv")

        if not os.path.exists(stores_file_path):
            raise FileNotFoundError(f"stores.csv not found at {stores_file_path}")

        print(f"stores.csv found at {stores_file_path}")
        return stores_file_path

    @task()
    def filter_us_stores(stores_path: str, output_dir: str = OUTPUT_DIR) -> str:
        """Filter the stores dataset to only include stores located in the United States."""
        df = pd.read_csv(stores_path)
        us_stores = df[df["Country"] == "United States"]
        us_stores_path = os.path.join(output_dir, "us_stores.csv")
        us_stores.to_csv(us_stores_path, index=False)

        print(f"Filtered US stores saved to {us_stores_path}")
        return us_stores_path

    @task()
    def remove_product_launch_date(
        product_path: str, output_dir: str = OUTPUT_DIR
    ) -> str:
        """Remove the product launch dates from the products dataset."""
        df = pd.read_csv(product_path)
        product_no_launch = df.drop(columns=["Launch_Date"])
        product_no_launch_path = os.path.join(output_dir, "product_no_launch.csv")
        product_no_launch.to_csv(product_no_launch_path, index=False)

        print(f"Filtered US stores saved to {product_no_launch_path}")
        return product_no_launch_path

    @task()
    def merge_apple_data(
        categories_path: str,
        products_path: str,
        sales_path: str,
        stores_path: str,
        intermediate_dir: str = INTERMEDIATE_DIR,
    ) -> str:
        """Merge the category, products (without the product launch date), sales, and US stores datasets into a single dataset called apple_data."""

        categories = pd.read_csv(categories_path)
        products = pd.read_csv(products_path)
        sales = pd.read_csv(sales_path)
        stores = pd.read_csv(stores_path)

        cat_prod = pd.merge(
            categories,
            products,
            left_on="category_id",
            right_on="Category_ID",
            how="inner",
        )

        cat_prod_sales = pd.merge(
            cat_prod, sales, left_on="Product_ID", right_on="product_id", how="inner"
        )

        apple_data = pd.merge(
            cat_prod_sales, stores, left_on="store_id", right_on="Store_ID", how="inner"
        )

        apple_data_path = os.path.join(intermediate_dir, "apple_data.csv")
        apple_data.to_csv(apple_data_path, index=False)

        print(f"Final merged dataset saved to {apple_data_path}")
        return apple_data_path

    @task()
    def move_datasets_to_intermediate(
        us_stores_path: str,
        product_no_launch_path: str,
        intermediate_dir: str = INTERMEDIATE_DIR,
    ) -> tuple[str, str]:
        """Move us_stores.csv and product_no_launch.csv into the intermediate_data folder."""

        us_stores_moved = os.path.join(intermediate_dir, "us_stores.csv")
        shutil.move(us_stores_path, us_stores_moved)
        print(f"Moved us_stores.csv to {us_stores_moved}")

        product_no_launch_moved = os.path.join(
            intermediate_dir, "product_no_launch.csv"
        )
        shutil.move(product_no_launch_path, product_no_launch_moved)
        print(f"Moved product_no_launch.csv to {product_no_launch_moved}")

        return us_stores_moved, product_no_launch_moved

    @task()
    def add_revenue_column(
        merged_path: str, intermediate_dir: str = INTERMEDIATE_DIR
    ) -> str:
        """Calculate the revenue and add it as a variable to apple_data."""
        df = pd.read_csv(merged_path)

        if "Price" not in df.columns or "quantity" not in df.columns:
            raise ValueError("Missing 'Price' or 'quantity' column in merged data.")

        df["revenue"] = df["Price"] * df["quantity"]

        revenue_path = os.path.join(intermediate_dir, "apple_data.csv")
        df.to_csv(revenue_path, index=False)

        print(f"Revenue column added and saved to {revenue_path}")
        return revenue_path

    @task()
    def aggregate_revenue_by_category(
        data_path: str, intermediate_dir: str = INTERMEDIATE_DIR
    ) -> str:
        """Find the total revenue generated by each product category."""
        df = pd.read_csv(data_path)

        if "category_name" not in df.columns or "revenue" not in df.columns:
            raise ValueError("Missing 'category_name' or 'revenue' column in data.")

        revenue_by_category = df.groupby("category_name", as_index=False)[
            "revenue"
        ].sum()

        revenue_by_category_path = os.path.join(
            intermediate_dir, "revenue_by_category.csv"
        )
        revenue_by_category.to_csv(revenue_by_category_path, index=False)

        print(f"Revenue by category saved to {revenue_by_category_path}")
        return revenue_by_category_path

    @task()
    def plot_revenue_by_category(data_path: str, results_dir: str = RESULTS_DIR) -> str:
        """Generate a bar plot to show the amount of revenue generated by each category."""

        df = pd.read_csv(data_path)

        if "category_name" not in df.columns or "revenue" not in df.columns:
            raise ValueError("Missing 'category_name' or 'revenue' column in data.")

        plt.figure(figsize=(10, 6))
        bars = plt.bar(
            df["category_name"], df["revenue"], label="Revenue", color="skyblue"
        )

        plt.title("Total US Revenue by Category")
        plt.xlabel("Category")
        plt.ylabel("Revenue (Hundred Million USD)")
        plt.xticks(rotation=45, ha="right")
        plt.legend()

        plot_path = os.path.join(results_dir, "revenue_by_category_plot.png")
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()

        print(f"Revenue plot saved to {plot_path}")
        return plot_path

    @task()
    def clear_intermediate_data(
        folder_path: str = "/opt/airflow/data/intermediate_data",
    ) -> None:
        """
        Delete all files and subdirectories inside the intermediate_data folder.
        Keeps the folder itself.
        """

        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

        print("Clean process completed!")

    apple_categories = fetch_categories()
    apple_products = fetch_products()
    apple_sales = fetch_sales()
    apple_stores = fetch_stores()

    us_stores = filter_us_stores(apple_stores)
    product_no_launch = remove_product_launch_date(apple_products)

    apple_data = merge_apple_data(
        categories_path=apple_categories,
        products_path=product_no_launch,
        sales_path=apple_sales,
        stores_path=us_stores,
    )

    moved_datasets = move_datasets_to_intermediate(
        us_stores_path=us_stores,
        product_no_launch_path=product_no_launch,
    )

    apple_data >> moved_datasets

    apple_data_with_revenue = add_revenue_column(apple_data)

    category_revenue = aggregate_revenue_by_category(apple_data_with_revenue)

    revenue_plot = plot_revenue_by_category(category_revenue)

    clean_folder = clear_intermediate_data(folder_path=INTERMEDIATE_DIR)

    revenue_plot >> clean_folder
