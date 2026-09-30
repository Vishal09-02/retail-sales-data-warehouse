# ============================================================
# RETAIL SALES DATA WAREHOUSE MINI PROJECT
# Star Schema + Snowflake Schema + Fact Constellation
# ETL + Data Cleaning + SQLite + Streamlit Dashboard
# ============================================================

import sqlite3
import random
import os
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import plotly.express as px


# ============================================================
# CONFIGURATION
# ============================================================

DB_NAME = "retail_warehouse.db"

random.seed(42)


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Retail Data Warehouse",
    page_icon="🛒",
    layout="wide"
)


# ============================================================
# GENERATE RAW DATA
# ============================================================

def generate_raw_data():

    customers = [
        [101, "Rahul Sharma", "Male", 24, "Pune", "Maharashtra"],
        [102, "Priya Patil", "Female", 29, "Mumbai", "Maharashtra"],
        [103, "Amit Joshi", "Male", 35, "Nashik", "Maharashtra"],
        [104, "Sneha Kulkarni", "Female", 27, "Pune", "Maharashtra"],
        [105, "Rohan Deshmukh", "Male", 31, "Nagpur", "Maharashtra"],
        [106, "Neha Shah", "Female", 26, "Ahmedabad", "Gujarat"],
        [107, "Vikas Mehta", "Male", 40, "Surat", "Gujarat"],
        [108, "Anjali Gupta", "Female", 33, "Delhi", "Delhi"],
        [109, "Karan Verma", "Male", 28, "Delhi", "Delhi"],
        [110, "Pooja Singh", "Female", 30, "Bangalore", "Karnataka"],
    ]

    products = [
        [201, "Laptop", "Electronics", "Computers", "Dell", 55000],
        [202, "Smartphone", "Electronics", "Mobile", "Samsung", 25000],
        [203, "Headphones", "Electronics", "Audio", "Sony", 3000],
        [204, "Keyboard", "Accessories", "Computer Accessories", "Logitech", 1500],
        [205, "Mouse", "Accessories", "Computer Accessories", "HP", 800],
        [206, "T-Shirt", "Clothing", "Men", "Puma", 1200],
        [207, "Jeans", "Clothing", "Men", "Levis", 2500],
        [208, "Shoes", "Footwear", "Sports", "Nike", 4500],
        [209, "Watch", "Accessories", "Watches", "Titan", 3500],
        [210, "Backpack", "Accessories", "Bags", "Skybags", 1800],
    ]

    stores = [
        [301, "SmartMart Pune", "Pune", "Maharashtra", "West"],
        [302, "SmartMart Mumbai", "Mumbai", "Maharashtra", "West"],
        [303, "SmartMart Delhi", "Delhi", "Delhi", "North"],
        [304, "SmartMart Bangalore", "Bangalore", "Karnataka", "South"],
        [305, "SmartMart Ahmedabad", "Ahmedabad", "Gujarat", "West"],
    ]

    customer_df = pd.DataFrame(
        customers,
        columns=[
            "Customer_ID",
            "Customer_Name",
            "Gender",
            "Age",
            "City",
            "State"
        ]
    )

    product_df = pd.DataFrame(
        products,
        columns=[
            "Product_ID",
            "Product_Name",
            "Category",
            "Subcategory",
            "Brand",
            "Unit_Price"
        ]
    )

    store_df = pd.DataFrame(
        stores,
        columns=[
            "Store_ID",
            "Store_Name",
            "City",
            "State",
            "Region"
        ]
    )

    # --------------------------------------------------------
    # Generate sales transactions
    # --------------------------------------------------------

    sales = []

    start_date = datetime(2025, 1, 1)

    for i in range(1, 1001):

        sale_id = i

        date = start_date + timedelta(
            days=random.randint(0, 364)
        )

        customer = random.choice(customers)
        product = random.choice(products)
        store = random.choice(stores)

        quantity = random.randint(1, 5)

        unit_price = product[5]

        discount = random.choice(
            [0, 5, 10, 15, 20]
        )

        sales_amount = quantity * unit_price

        discount_amount = (
            sales_amount * discount / 100
        )

        net_sales = (
            sales_amount - discount_amount
        )

        sales.append([
            sale_id,
            date.strftime("%Y-%m-%d"),
            customer[0],
            product[0],
            store[0],
            quantity,
            unit_price,
            discount,
            sales_amount,
            discount_amount,
            net_sales
        ])

    sales_df = pd.DataFrame(
        sales,
        columns=[
            "Sale_ID",
            "Date",
            "Customer_ID",
            "Product_ID",
            "Store_ID",
            "Quantity",
            "Unit_Price",
            "Discount",
            "Sales_Amount",
            "Discount_Amount",
            "Net_Sales"
        ]
    )

    # Add some duplicate records intentionally
    duplicate_rows = sales_df.sample(10, random_state=1)

    sales_df = pd.concat(
        [sales_df, duplicate_rows],
        ignore_index=True
    )

    # Add some missing discount values
    sales_df.loc[
        sales_df.sample(10, random_state=2).index,
        "Discount"
    ] = None

    return (
        sales_df,
        customer_df,
        product_df,
        store_df
    )


# ============================================================
# ETL PROCESS
# ============================================================

def perform_etl(
    sales_df,
    customer_df,
    product_df,
    store_df
):

    # --------------------------------------------------------
    # EXTRACT
    # --------------------------------------------------------

    sales = sales_df.copy()
    customers = customer_df.copy()
    products = product_df.copy()
    stores = store_df.copy()

    # --------------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------------

    # Remove duplicates
    sales = sales.drop_duplicates(
        subset=["Sale_ID"]
    )

    # Convert date
    sales["Date"] = pd.to_datetime(
        sales["Date"]
    )

    # Handle missing values
    sales["Discount"] = sales["Discount"].fillna(0)

    # Standardize numerical columns
    sales["Quantity"] = sales["Quantity"].astype(int)

    sales["Unit_Price"] = pd.to_numeric(
        sales["Unit_Price"]
    )

    # Recalculate values after cleaning
    sales["Sales_Amount"] = (
        sales["Quantity"]
        * sales["Unit_Price"]
    )

    sales["Discount_Amount"] = (
        sales["Sales_Amount"]
        * sales["Discount"]
        / 100
    )

    sales["Net_Sales"] = (
        sales["Sales_Amount"]
        - sales["Discount_Amount"]
    )

    # Clean text columns
    customers["Customer_Name"] = (
        customers["Customer_Name"]
        .str.strip()
    )

    products["Product_Name"] = (
        products["Product_Name"]
        .str.strip()
    )

    stores["Store_Name"] = (
        stores["Store_Name"]
        .str.strip()
    )

    # --------------------------------------------------------
    # CREATE DATE DIMENSION
    # --------------------------------------------------------

    dates = sales["Date"].drop_duplicates()

    date_dimension = pd.DataFrame()

    date_dimension["Date_Key"] = (
        dates.dt.strftime("%Y%m%d").astype(int)
    )

    date_dimension["Full_Date"] = dates

    date_dimension["Day"] = dates.dt.day

    date_dimension["Month"] = dates.dt.month

    date_dimension["Month_Name"] = (
        dates.dt.strftime("%B")
    )

    date_dimension["Quarter"] = (
        "Q" + dates.dt.quarter.astype(str)
    )

    date_dimension["Year"] = dates.dt.year

    date_dimension = date_dimension.reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # STAR SCHEMA DIMENSIONS
    # --------------------------------------------------------

    dim_customer = customers.copy()

    dim_customer.insert(
        0,
        "Customer_Key",
        range(1, len(dim_customer) + 1)
    )

    dim_product = products.copy()

    dim_product.insert(
        0,
        "Product_Key",
        range(1, len(dim_product) + 1)
    )

    dim_store = stores.copy()

    dim_store.insert(
        0,
        "Store_Key",
        range(1, len(dim_store) + 1)
    )

    # --------------------------------------------------------
    # CREATE LOOKUP DICTIONARIES
    # --------------------------------------------------------

    customer_lookup = dict(
        zip(
            dim_customer["Customer_ID"],
            dim_customer["Customer_Key"]
        )
    )

    product_lookup = dict(
        zip(
            dim_product["Product_ID"],
            dim_product["Product_Key"]
        )
    )

    store_lookup = dict(
        zip(
            dim_store["Store_ID"],
            dim_store["Store_Key"]
        )
    )

    date_lookup = dict(
        zip(
            date_dimension["Full_Date"],
            date_dimension["Date_Key"]
        )
    )

    # --------------------------------------------------------
    # CREATE FACT SALES
    # --------------------------------------------------------

    fact_sales = sales.copy()

    fact_sales["Date_Key"] = (
        fact_sales["Date"].map(date_lookup)
    )

    fact_sales["Customer_Key"] = (
        fact_sales["Customer_ID"]
        .map(customer_lookup)
    )

    fact_sales["Product_Key"] = (
        fact_sales["Product_ID"]
        .map(product_lookup)
    )

    fact_sales["Store_Key"] = (
        fact_sales["Store_ID"]
        .map(store_lookup)
    )

    fact_sales = fact_sales[
        [
            "Sale_ID",
            "Date_Key",
            "Customer_Key",
            "Product_Key",
            "Store_Key",
            "Quantity",
            "Sales_Amount",
            "Discount_Amount",
            "Net_Sales"
        ]
    ]

    # --------------------------------------------------------
    # CREATE SNOWFLAKE TABLES
    # --------------------------------------------------------

    dim_category = (
        products[
            ["Category"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_category.insert(
        0,
        "Category_Key",
        range(1, len(dim_category) + 1)
    )

    dim_subcategory = (
        products[
            ["Subcategory", "Category"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_subcategory.insert(
        0,
        "Subcategory_Key",
        range(1, len(dim_subcategory) + 1)
    )

    dim_brand = (
        products[
            ["Brand"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_brand.insert(
        0,
        "Brand_Key",
        range(1, len(dim_brand) + 1)
    )

    dim_region = (
        stores[
            ["Region"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_region.insert(
        0,
        "Region_Key",
        range(1, len(dim_region) + 1)
    )

    return {
        "dim_date": date_dimension,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "fact_sales": fact_sales,
        "dim_category": dim_category,
        "dim_subcategory": dim_subcategory,
        "dim_brand": dim_brand,
        "dim_region": dim_region
    }


# ============================================================
# CREATE FACT RETURNS
# ============================================================

def create_returns(fact_sales):

    returns = fact_sales.sample(
        frac=0.08,
        random_state=10
    ).copy()

    returns = returns[
        [
            "Sale_ID",
            "Date_Key",
            "Customer_Key",
            "Product_Key",
            "Store_Key",
            "Quantity",
            "Net_Sales"
        ]
    ]

    returns["Returned_Quantity"] = (
        returns["Quantity"].apply(
            lambda x: random.randint(
                1, max(1, x)
            )
        )
    )

    returns["Refund_Amount"] = (
        returns["Net_Sales"]
        * returns["Returned_Quantity"]
        / returns["Quantity"]
    )

    returns.insert(
        0,
        "Return_Key",
        range(1, len(returns) + 1)
    )

    return returns[
        [
            "Return_Key",
            "Date_Key",
            "Customer_Key",
            "Product_Key",
            "Store_Key",
            "Returned_Quantity",
            "Refund_Amount"
        ]
    ]


# ============================================================
# CREATE SQLITE DATABASE
# ============================================================

def create_database():

    sales_df, customer_df, product_df, store_df = (
        generate_raw_data()
    )

    warehouse = perform_etl(
        sales_df,
        customer_df,
        product_df,
        store_df
    )

    fact_returns = create_returns(
        warehouse["fact_sales"]
    )

    warehouse["fact_returns"] = fact_returns

    connection = sqlite3.connect(DB_NAME)

    # --------------------------------------------------------
    # Load all warehouse tables
    # --------------------------------------------------------

    for table_name, dataframe in warehouse.items():

        dataframe.to_sql(
            table_name,
            connection,
            if_exists="replace",
            index=False
        )

    connection.commit()
    connection.close()

    return warehouse


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():

    return sqlite3.connect(
        DB_NAME
    )


# ============================================================
# DASHBOARD
# ============================================================

def dashboard():

    st.title(
        "🛒 Retail Sales Data Warehouse"
    )

    st.markdown(
        """
        ### Retail Sales & Customer Purchasing Analysis

        This project demonstrates:

        - ETL operations
        - Data cleaning
        - Star Schema
        - Snowflake Schema
        - Fact Constellation Schema
        - SQL-based analysis
        - Interactive dashboards
        """
    )

    # --------------------------------------------------------
    # CREATE DATABASE IF IT DOES NOT EXIST
    # --------------------------------------------------------

    if not os.path.exists(DB_NAME):

        with st.spinner(
            "Creating retail data warehouse..."
        ):
            create_database()

        st.success(
            "Data warehouse created successfully!"
        )

    connection = get_connection()

    # --------------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------------

    fact_sales = pd.read_sql(
        "SELECT * FROM fact_sales",
        connection
    )

    fact_returns = pd.read_sql(
        "SELECT * FROM fact_returns",
        connection
    )

    dim_product = pd.read_sql(
        "SELECT * FROM dim_product",
        connection
    )

    dim_customer = pd.read_sql(
        "SELECT * FROM dim_customer",
        connection
    )

    dim_store = pd.read_sql(
        "SELECT * FROM dim_store",
        connection
    )

    dim_date = pd.read_sql(
        "SELECT * FROM dim_date",
        connection
    )

    connection.close()

    # --------------------------------------------------------
    # MERGE DATA FOR ANALYSIS
    # --------------------------------------------------------

    analysis = fact_sales.merge(
        dim_product[
            [
                "Product_Key",
                "Product_Name",
                "Category",
                "Subcategory",
                "Brand"
            ]
        ],
        on="Product_Key"
    )

    analysis = analysis.merge(
        dim_customer[
            [
                "Customer_Key",
                "Customer_Name",
                "Gender",
                "Age",
                "City",
                "State"
            ]
        ],
        on="Customer_Key"
    )

    analysis = analysis.merge(
        dim_store[
            [
                "Store_Key",
                "Store_Name",
                "City",
                "State",
                "Region"
            ]
        ],
        on="Store_Key",
        suffixes=("_Customer", "_Store")
    )

    analysis = analysis.merge(
        dim_date[
            [
                "Date_Key",
                "Full_Date",
                "Month",
                "Month_Name",
                "Quarter",
                "Year"
            ]
        ],
        on="Date_Key"
    )

    # --------------------------------------------------------
    # KPI SECTION
    # --------------------------------------------------------

    total_sales = analysis[
        "Net_Sales"
    ].sum()

    total_orders = analysis[
        "Sale_ID"
    ].nunique()

    total_quantity = analysis[
        "Quantity"
    ].sum()

    total_customers = analysis[
        "Customer_Key"
    ].nunique()

    total_products = analysis[
        "Product_Key"
    ].nunique()

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric(
        "💰 Total Sales",
        f"₹{total_sales:,.0f}"
    )

    col2.metric(
        "🧾 Orders",
        f"{total_orders:,}"
    )

    col3.metric(
        "📦 Units Sold",
        f"{total_quantity:,}"
    )

    col4.metric(
        "👥 Customers",
        f"{total_customers:,}"
    )

    col5.metric(
        "🛍️ Products",
        f"{total_products:,}"
    )

    st.divider()

    # --------------------------------------------------------
    # SIDEBAR FILTER
    # --------------------------------------------------------

    st.sidebar.header(
        "🔎 Filters"
    )

    categories = [
        "All"
    ] + sorted(
        analysis["Category"].unique().tolist()
    )

    selected_category = st.sidebar.selectbox(
        "Select Category",
        categories
    )

    if selected_category != "All":

        analysis = analysis[
            analysis["Category"]
            == selected_category
        ]

    # --------------------------------------------------------
    # MONTHLY SALES
    # --------------------------------------------------------

    st.subheader(
        "📈 Monthly Sales Trend"
    )

    monthly_sales = (
        analysis
        .groupby(
            ["Year", "Month"]
        )["Net_Sales"]
        .sum()
        .reset_index()
    )

    monthly_sales["Month_Name"] = (
        pd.to_datetime(
            monthly_sales["Month"],
            format="%m"
        ).dt.strftime("%b")
    )

    monthly_sales["Period"] = (
        monthly_sales["Year"].astype(str)
        + "-"
        + monthly_sales["Month_Name"]
    )

    fig = px.line(
        monthly_sales,
        x="Period",
        y="Net_Sales",
        markers=True,
        title="Monthly Net Sales"
    )

    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Net Sales (₹)"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    # --------------------------------------------------------
    # CATEGORY SALES
    # --------------------------------------------------------

    col1, col2 = st.columns(2)

    with col1:

        category_sales = (
            analysis
            .groupby("Category")[
                "Net_Sales"
            ]
            .sum()
            .reset_index()
        )

        fig_category = px.bar(
            category_sales,
            x="Category",
            y="Net_Sales",
            color="Category",
            title="Sales by Product Category"
        )

        st.plotly_chart(
            fig_category,
            use_container_width=True
        )

    # --------------------------------------------------------
    # REGION SALES
    # --------------------------------------------------------

    with col2:

        region_sales = (
            analysis
            .groupby("Region")[
                "Net_Sales"
            ]
            .sum()
            .reset_index()
        )

        fig_region = px.pie(
            region_sales,
            names="Region",
            values="Net_Sales",
            title="Sales by Region"
        )

        st.plotly_chart(
            fig_region,
            use_container_width=True
        )

    # --------------------------------------------------------
    # TOP PRODUCTS
    # --------------------------------------------------------

    st.subheader(
        "🏆 Top 10 Products"
    )

    top_products = (
        analysis
        .groupby(
            "Product_Name"
        )
        .agg(
            Units_Sold=("Quantity", "sum"),
            Revenue=("Net_Sales", "sum")
        )
        .reset_index()
        .sort_values(
            "Revenue",
            ascending=False
        )
        .head(10)
    )

    fig_products = px.bar(
        top_products,
        x="Revenue",
        y="Product_Name",
        orientation="h",
        color="Revenue",
        title="Top 10 Products by Revenue"
    )

    st.plotly_chart(
        fig_products,
        use_container_width=True
    )

    # --------------------------------------------------------
    # CUSTOMER ANALYSIS
    # --------------------------------------------------------

    st.subheader(
        "👥 Customer Purchasing Behavior"
    )

    customer_analysis = (
        analysis
        .groupby(
            [
                "Customer_Name",
                "Gender",
                "City_Customer"
            ]
        )
        .agg(
            Transactions=(
                "Sale_ID",
                "nunique"
            ),
            Units_Purchased=(
                "Quantity",
                "sum"
            ),
            Total_Purchase=(
                "Net_Sales",
                "sum"
            )
        )
        .reset_index()
        .sort_values(
            "Total_Purchase",
            ascending=False
        )
    )

    st.dataframe(
        customer_analysis.head(10),
        use_container_width=True
    )

    # --------------------------------------------------------
    # STORE ANALYSIS
    # --------------------------------------------------------

    st.subheader(
        "🏬 Store Performance"
    )

    store_analysis = (
        analysis
        .groupby(
            "Store_Name"
        )
        .agg(
            Orders=(
                "Sale_ID",
                "nunique"
            ),
            Units_Sold=(
                "Quantity",
                "sum"
            ),
            Revenue=(
                "Net_Sales",
                "sum"
            )
        )
        .reset_index()
        .sort_values(
            "Revenue",
            ascending=False
        )
    )

    st.dataframe(
        store_analysis,
        use_container_width=True
    )

    # --------------------------------------------------------
    # RETURN ANALYSIS
    # --------------------------------------------------------

    st.subheader(
        "↩️ Product Return Analysis"
    )

    return_analysis = fact_returns.merge(
        dim_product[
            [
                "Product_Key",
                "Product_Name"
            ]
        ],
        on="Product_Key"
    )

    return_summary = (
        return_analysis
        .groupby(
            "Product_Name"
        )
        .agg(
            Returned_Quantity=(
                "Returned_Quantity",
                "sum"
            ),
            Refund_Amount=(
                "Refund_Amount",
                "sum"
            )
        )
        .reset_index()
        .sort_values(
            "Returned_Quantity",
            ascending=False
        )
    )

    st.dataframe(
        return_summary,
        use_container_width=True
    )

    # --------------------------------------------------------
    # DATA PREVIEW
    # --------------------------------------------------------

    st.subheader(
        "🗄️ Data Warehouse Tables"
    )

    table_choice = st.selectbox(
        "Select a table",
        [
            "fact_sales",
            "fact_returns",
            "dim_date",
            "dim_customer",
            "dim_product",
            "dim_store"
        ]
    )

    connection = get_connection()

    selected_table = pd.read_sql(
        f"SELECT * FROM {table_choice} LIMIT 50",
        connection
    )

    connection.close()

    st.dataframe(
        selected_table,
        use_container_width=True
    )

    # --------------------------------------------------------
    # SCHEMA INFORMATION
    # --------------------------------------------------------

    st.subheader(
        "🏗️ Data Warehouse Architecture"
    )

    st.markdown(
        """
        ### ⭐ Star Schema

        ```text
                     DIM_DATE
                         |
                         |
        DIM_CUSTOMER -- FACT_SALES -- DIM_PRODUCT
                         |
                         |
                     DIM_STORE
        ```

        ### ❄️ Snowflake Schema

        ```text
        FACT_SALES
             |
        DIM_PRODUCT
          /  |  \\
         /   |   \\
        CATEGORY SUBCATEGORY BRAND

        DIM_STORE
             |
          REGION
        ```

        ### 🌟 Fact Constellation Schema

        ```text
                    DIM_DATE
                    /      \\
                   /        \\
            FACT_SALES     FACT_RETURNS
             / | | \\       / | | \\
            /  | |  \\     /  | |  \\
        CUSTOMER PRODUCT STORE CUSTOMER PRODUCT STORE
        ```
        """
    )

    # --------------------------------------------------------
    # ETL INFORMATION
    # --------------------------------------------------------

    st.subheader(
        "🔄 ETL Process"
    )

    etl_col1, etl_col2, etl_col3 = st.columns(3)

    with etl_col1:

        st.info(
            """
            ### 1. EXTRACT

            Data collected from:

            - CSV
            - Excel
            - Transaction records
            """
        )

    with etl_col2:

        st.warning(
            """
            ### 2. TRANSFORM

            Operations performed:

            - Duplicate removal
            - Missing-value handling
            - Data type conversion
            - Data standardization
            - Sales calculations
            """
        )

    with etl_col3:

        st.success(
            """
            ### 3. LOAD

            Cleaned data loaded into:

            - Dimension tables
            - Fact tables
            - SQLite warehouse
            """
        )

    # --------------------------------------------------------
    # PROJECT INFORMATION
    # --------------------------------------------------------

    st.divider()

    st.caption(
        "Retail Sales & Customer Purchasing Analysis "
        "using Data Warehouse and ETL"
    )

    st.caption(
        "Mini Project - Group C"
    )


# ============================================================
# MAIN PROGRAM
# ============================================================

if __name__ == "__main__":

    dashboard()
