# IDS 706 - Data Engineering - Week 10 - Airflow Pipeline

## Project Overview

This project is dedicated to creating an Airflow pipeline that receives data, processes it, and creates a visualization for analysis.

## Data Overview

This project was completed using data on Apple products. Datasets that were used include:

1. category.csv: Categories of the products.
2. products.csv: General information on the products, such as the name and price of the products.
3. sales.csv: Information on product sales, such as number of products sold and date of sales.
4. stores.csv: Apple store information, such as location (city and country).

The data was obtained from Kaggle and can be found using this link: https://www.kaggle.com/datasets/amangarg08/apple-retail-sales-dataset?select=sales.csv

## Pipeline Overivew

The structure of the pipeline can be found here:

<img width="2692" height="1093" alt="Screenshot 2025-11-07 213854" src="https://github.com/user-attachments/assets/586af926-fc7e-43e2-9ccc-d05dc8fcd857" />


This pipeline performs the following tasks:

### Fetching the Data

This task focuses on verifying that the four datasets listed in the Data Overview section exist in the data folder and provides the file path of the dataset to be used on future tasks. Each dataset has its own task for fetching. Since the products and stores datasets require some transformations, they will be fetched before the category and sales datasets and transformed while the category and sales datasets are being fetched.

### Pre-Merge Transformations

There are two transformations that occur before the datasets are merged:

1. The product launch dates were removed from the products dataset, since it will not be used in any future analysis. Removing this variable now will make merged dataset cleaner and easier to analyze.

2. The stores dataset was filtered to only include US stores, since we only want to focus on US stores for the analysis.

### Merge the Datasets

The category and sales datasets are merged with the transformed products and stores datasets to create a single dataset containing all the relevant data. The category and product datasets were merged on the category ID, the resulting dataset was merged with the sales dataset on the product ID, and the resulting dataset from this merge was merged with the stores datasets on the store ID. The final merged dataset is moved to a subfolder called intermediate_data.

### Move the Transformed Products and Stores Datasets to the Intermediate Data Folder

Move the transformed datasets to the intermediate data folder to maintain organization. Note that this step is happening while post-merge transformations are occurring.

### Add Revenue to the Merged Dataset

Calculate the revenue by take the product of the price of a product and the quantity sold, and store the results in a new column in the merged dataset.

### Find the Total Revenue for each Product Category

Using the revenue calculated in the previous task, group the data based on the product categories and take the sum of the revenue within each group.

### Plot the Revenue by Category

Create a bar that shows the total US revenue (in hundred million US dollars) for each product category. This will be the main resource for the analysis.

### Clean Up the Intermediate Data

Remove all intermediate datasets (i.e., datasets from pre-merge transformations, the merged dataset, and datasets from post-merge transformations).

This is the performance of the last run of the pipeline:

<img width="2693" height="1130" alt="Screenshot 2025-11-07 214016" src="https://github.com/user-attachments/assets/4a52ae35-90a3-4be4-87f3-e56586d12424" />

## Analysis

Here is the plot generated for the analysis:

<img width="1000" height="600" alt="revenue_by_category_plot" src="https://github.com/user-attachments/assets/c5d63986-1995-4afa-8f81-980d16cf54a4" />


Based on this plot, tablets and accessories generate the most revenue. Tablets may be popular because they are the perfect mix of computer and phone. They provide a wider screen than a phone, but the screen is still small enough to be portable. As for accessories, it's very common for people to by accessories for their devices because it's a way for them to express themselves.
