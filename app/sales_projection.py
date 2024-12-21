import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import plotly.graph_objects as go
from prophet import Prophet
import plotly.express as px
from azure.storage.blob import BlobServiceClient
import os

local_file_path = "truck_data.parquet"

if not os.path.exists(local_file_path):
    connection_string =  os.getenv('AZURE_CONNECTION_STRING')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    blob_name = os.getenv('AZURE_BLOB_NAME')

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(local_file_path, "wb") as file:
        file.write(blob_client.download_blob().readall())

selected_columns = ['TRUCK_BRAND_NAME', 'MENU_TYPE', 'PRICE', 'CITY', 'ITEM_CATEGORY', 'ITEM_SUBCATEGORY', 'ORDER_TOTAL', 'ORDER_TS']

spark = SparkSession.builder \
    .appName('tasty-trails') \
    .config("spark.master", "local[*]") \
    .getOrCreate()

def load_sample_data(source_file, selected_columns):
    """
    Loads a sample of data and extracts unique values and date range.

    Returns:
        tuple: A tuple with unique truck brands, item categories, cities, and min/max years.
    """

    df_spark_sample = spark.read.parquet(source_file).select(selected_columns).sample(fraction=0.1, seed=17)
    
    df_spark_sample = df_spark_sample.withColumn('DATE', col('ORDER_TS').cast('date')) \
                       .withColumn('HOUR', hour(col('ORDER_TS'))) \
                       .drop('ORDER_TS')
    
    def get_unique_values(column_name):
        unique_values = [row[column_name] for row in df_spark_sample.select(column_name).distinct().collect()]
        return ['All'] + unique_values

    truck_brands = get_unique_values('TRUCK_BRAND_NAME')
    cities = get_unique_values('CITY')
    item_categories = get_unique_values('ITEM_CATEGORY')

    min_year = df_spark_sample.select(F.year(F.min('DATE')).alias('min_year')).collect()[0]['min_year']
    max_year = df_spark_sample.select(F.year(F.max('DATE')).alias('max_year')).collect()[0]['max_year']
        
    return truck_brands, item_categories, cities, min_year, max_year

truck_brands, categories, cities, min_year, max_year = load_sample_data(local_file_path, selected_columns)

@st.cache_resource
def load_data(source_file, selected_columns, truck_brand, city, category):
    """
    Loads and filters data based on selected criteria for truck brand, city, and category.

    Returns:
        DataFrame: A filtered Spark DataFrame based on the user inputs.
    """

    df_spark = spark.read.parquet(source_file).select(selected_columns)
    
    df_spark = df_spark.withColumn('DATE', col('ORDER_TS').cast('date')) \
                       .withColumn('HOUR', hour(col('ORDER_TS'))) \
                       .drop('ORDER_TS')
    
    if 'All' not in truck_brand:
        df_spark = df_spark.filter(F.col('TRUCK_BRAND_NAME').isin(truck_brand))
    if 'All' not in city:
        df_spark = df_spark.filter(F.col('CITY').isin(city))
    if 'All' not in category:
        df_spark = df_spark.filter(F.col('ITEM_CATEGORY').isin(category))
    
    df_spark = df_spark.repartition(2, "CITY")

    return df_spark

def app():
    st.title('Explore Tasty Bites Sales Performance and Projections (Poland)')

    st.divider()

    st.sidebar.title('Filters')

    filtered_data = None

    selected_truck_brand = st.sidebar.selectbox('Select Truck Brand', options = truck_brands, index = 0)
    selected_city = st.sidebar.radio('Select City', options = cities, index = 0, horizontal = True)
    selected_categories = st.sidebar.multiselect('Select Item Category', options = categories, default = 'All')
    
    if min_year == max_year:
        st.sidebar.warning('Data is available only for the year: {}'.format(min_year))
        selected_year_range = (min_year, max_year)
    else:
        selected_year_range = st.sidebar.slider('Select Year Range', min_value = min_year, 
                                                max_value = max_year, value = (min_year, max_year)
                                                )

    df_spark = load_data(local_file_path, selected_columns, selected_truck_brand, selected_city, selected_categories)

    if st.sidebar.button('Apply Selection'):
        filtered_data = df_spark

    if filtered_data is not None:
        filtered_data = df_spark.filter(
                (F.year(F.col('DATE')) >= selected_year_range[0]) &
                (F.year(F.col('DATE')) <= selected_year_range[1])
            )


        summary_df = filtered_data.groupBy('ITEM_SUBCATEGORY') \
                                  .agg(
                                        F.avg('PRICE').alias('avg_spending'),
                                        F.sum('PRICE').alias('total_sales')
                                    )
        summary_df_pd = summary_df.toPandas()

        # Function to create sparkline plot
        def plot_sparkline(data):
            """
            Creates a sparkline plot for daily sales.

            Returns:
                Figure: A Plotly figure displaying the sales trend as a sparkline.
            """

            fig_spark = go.Figure(
                data=go.Scatter(
                    x = data['DATE'],  
                    y = data['daily_total'], 
                    mode = 'lines',
                    fill = 'tozeroy',
                    line_color = 'red',
                    fillcolor = 'pink',
                ),
            )
            fig_spark.update_traces(hovertemplate='Total Sales: $ %{y:.2f}')
            fig_spark.update_xaxes(visible = False, fixedrange = True)
            fig_spark.update_yaxes(visible = False, fixedrange = True)
            fig_spark.update_layout(
                showlegend=False,
                plot_bgcolor='white',
                height=50,
                margin=dict(t = 10, l = 0, b = 0, r = 0, pad = 0),
            )
            return fig_spark

        def cards(item_subcategory, total_sales, avg_spending, daily_total, option_type):
            """
            Displays a card with sales data, average spending, and a sparkline.
            """

            with st.container(border=True):
                tl, tr = st.columns([2, 1])
                bl, br = st.columns([1, 1])

                icons = {
                    'Hot Option': '🌶️',  
                    'Cold Option': '🍦', 
                    'Warm Option': '🍜'   
                }

                tl.markdown(f'**{icons.get(option_type, "")} {item_subcategory}**')

                tr.markdown(f'**Total Sales**')
                tr.markdown(f'$ {total_sales:,.0f}')    

                with bl:
                    st.markdown(f'**Average Spending**')
                    st.markdown(f'$ {avg_spending:,.2f}')

                with br:
                    fig_spark = plot_sparkline(daily_total)
                    st.plotly_chart(fig_spark, config=dict(displayModeBar = False), use_container_width = True)
 

        def display_metrics(summary_df_pd):
            """
            Displays metrics for top 3 item subcategories with total sales, spending, and a sparkline.
            """

            col1, col2, col3 = st.columns(3)

            for idx, (_, row) in enumerate(summary_df_pd.iterrows()):
                if idx >= 3: 
                    break
                item_subcategory = row['ITEM_SUBCATEGORY']
                total_sales = row['total_sales']
                avg_spending = row['avg_spending']

                if 'hot' in item_subcategory.lower():  
                    option_type = 'Hot Option'
                elif 'cold' in item_subcategory.lower(): 
                    option_type = 'Cold Option'
                else:
                    option_type = 'Warm Option' 

                daily_totals_subcategory = filtered_data.filter(F.col('ITEM_SUBCATEGORY') == item_subcategory) \
                    .groupBy('DATE').agg(F.sum('PRICE').alias('daily_total')) \
                    .orderBy('DATE') \
                    .toPandas()

                with [col1, col2, col3][idx]: 
                    cards(item_subcategory, total_sales, avg_spending, daily_totals_subcategory, option_type)
                    
        st.markdown('### The 3 Product Subcategories Sold by Trucks')
        display_metrics(summary_df_pd)

        df_grouped = filtered_data.groupBy('DATE', 'ITEM_CATEGORY').agg(F.sum('PRICE').alias('PRICE'))

        df_pivot = df_grouped.groupBy('DATE').pivot('ITEM_CATEGORY').sum('PRICE').fillna(0)

        df_pandas = df_pivot.toPandas()
        df_pandas = df_pandas.map(lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
        df_pandas['DATE'] = pd.to_datetime(df_pandas['DATE'])
        df_pandas = df_pandas.resample('W', on = 'DATE').sum().reset_index()
        df_pandas = df_pandas.iloc[:-1] 

        forecast_results = []
        for category in df_pandas.columns[1:]: 
            category_data = df_pandas[['DATE', category]].rename(columns = {category: 'PRICE'})
            category_data.columns = ['ds', 'y']
            
            model = Prophet(yearly_seasonality=True, weekly_seasonality = False, daily_seasonality = False)
            model.fit(category_data)
            
            future = model.make_future_dataframe(periods=365)
            forecast = model.predict(future)

            forecast['ITEM_CATEGORY'] = category
            forecast['Type'] = 'Forecast'
            category_data['ITEM_CATEGORY'] = category
            category_data['Type'] = 'Actual'

            forecast_results.append(pd.concat([category_data, forecast[['ds', 'yhat', 'ITEM_CATEGORY', 'Type']].rename(columns={'yhat': 'y'})]))

        combined_forecast_df = pd.concat(forecast_results)

        fig = px.line(
            combined_forecast_df,
            x = 'ds',
            y = 'y',
            color = 'ITEM_CATEGORY',
            line_dash = 'Type',  
            labels = {'ds': 'Date', 'y': 'Price'}
        )

        fig.update_layout(
            xaxis_title = 'Date',
            yaxis_title = 'Total Sales',
            legend_title = 'Category & Type',
            height=650
        )

        st.markdown('### Sales Forecast for the next year with actual data')
        st.plotly_chart(fig)

    else:
        st.write("Please select filters and click 'Apply Selection' to view results.")
