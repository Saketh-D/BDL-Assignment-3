from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import apache_beam as beam
import pandas as pd
from apache_beam.runners.direct.direct_runner import DirectRunner
import geopandas as gpd
import geodatasets as gds
import matplotlib.pyplot as plt

# Set this to the location of the archive file
archive_file_path ="/home/saketh/Desktop/assignment2/required_location/archive.zip"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}

dag = DAG(
    dag_id = 'task2_dag',
    default_args=default_args,
    description='A DAG to process archives and unzip contents into CSV files',
    schedule_interval='*/1 * * * *',  # Runs every minute
    catchup=False
)

# File Sensor for 1. Wait for the archive to be available
file_sensor_task = FileSensor(
    task_id='wait_for_archive',
    poke_interval=1,  # Check every second
    timeout=5,  # Stop the pipeline if not available within 5 seconds
    filepath=archive_file_path,  # Archive file path
    dag=dag
)

archive_bash_cmd = f'''
if $(file {archive_file_path} | grep -q 'Zip archive'); then
    mkdir -p /home/saketh/Desktop/assignment2/csv_files && 
    unzip {archive_file_path} -d /home/saketh/Desktop/assignment2/csv_files;
else
    echo "Not a valid archive file"
    exit 1
fi
'''

# Bash operator for 2. Check if the file is a valid archive and unzip
unzip_task = BashOperator(
    task_id='unzip_archive',
    bash_command=archive_bash_cmd,
    dag=dag
)


# function for 3. Extracting and filtering data from CSV
def extract_and_filter_csv(data):
    # Assuming data is a single row of CSV
    lat = data['Latitude']
    lon = data['Longitude']
    # Extract required fields
    windspeed = data['Windspeed']
    bulb_temperature = data['BulbTemperature']
    # returning a tuple containing lat, lon and a list of the hourly data from the required fields
    return (lat, lon, [[windspeed, bulb_temperature]])


# function for 3. extract and filter data using Apache Beam
def process_data_with_beam(file_path):
    with beam.Pipeline(runner=DirectRunner()) as pipeline:
        # Read CSV file
        data = (
            pipeline
            | 'ReadCSV' >> beam.io.ReadFromText(file_path)
            | 'ParseCSV' >> beam.Map(lambda row: pd.DataFrame([row.split(',')], columns=['Latitude', 'Longitude', 'Windspeed', 'BulbTemperature']))
        )
        
        # Filter and process data
        processed_data = (
            data
            | 'ExtractAndFilter' >> beam.Map(extract_and_filter_csv)
        )
        

# 3. PythonOperator
extract_contents_task = PythonOperator(
    task_id='extract_and_filter_data_with_beam',
    python_callable=process_data_with_beam,
    dag=dag,
)



# function for 4. Computing monthly averages
def compute_monthly_averages(data):
    # Assuming data is a list of tuples of the form (lat, lon, hourly_data)
    # Compute monthly averages for each field
    monthly_averages = []
    for lat, lon, hourly_data in data:
        monthly_avg = []
        # Assuming hourly_data is a list of lists with hourly data for each month
        for month_data in hourly_data:
            monthly_avg.append(sum(month_data) / len(month_data))
        monthly_averages.append((lat, lon, monthly_avg))
    return monthly_averages


# function for 4. compute monthly averages using Apache Beam
def compute_monthly_averages_with_beam():
    with beam.Pipeline(runner=DirectRunner()) as pipeline:
        # Read processed data
        processed_data = (
            pipeline
            | 'ReadProcessedData' >> beam.io.ReadFromText('/path/to/processed_data.csv')
            | 'ParseProcessedData' >> beam.Map(lambda row: eval(row))  # Assuming processed data is saved as text
        )
        
        # Compute monthly averages
        monthly_averages = (
            processed_data
            | 'ComputeMonthlyAverages' >> beam.Map(compute_monthly_averages)
        )


# 4. Python Operator for computing monthly averages using Apache Beam
compute_averages_task = PythonOperator(
    task_id='compute_monthly_averages_with_beam',
    python_callable=compute_monthly_averages_with_beam,
    dag=dag,
)


# function for 5. Create visualization using geopandas and export to PNG
def create_visualization(data):
    # Assuming data is a list of tuples of the form (lat, lon, field_data)
    for lat, lon, field_data in data:
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame({'Latitude': [lat], 'Longitude': [lon], 'FieldData': [field_data]})
        gdf = gdf.set_crs(epsg=4326)
        
        # Plot heatmap
        fig, ax = plt.subplots()
        gdf.plot(ax=ax, column='FieldData', cmap='viridis', legend=True)
        ax.set_title('Heatmap of Field Data')
        
        # Use geodatasets for basemap
        gds.basemap(ax)
        plt.savefig(f'/path/to/heatmap_{lat}_{lon}.png')
        plt.close()


# function for 5. create visualization and export to PNG
def create_visualization_with_beam():
    with beam.Pipeline(runner=DirectRunner()) as pipeline:
        # Read processed data
        processed_data = (
            pipeline
            | 'ReadProcessedData' >> beam.io.ReadFromText('/path/to/processed_data.csv')
            | 'ParseProcessedData' >> beam.Map(lambda row: eval(row))  # Assuming processed data is saved as text
        )
        
        # Create visualization
        visualization = (
            processed_data
            | 'CreateVisualization' >> beam.Map(create_visualization)
        )


# 5. PythonOperator
create_plots_task = PythonOperator(
    task_id='create_visualization_with_beam',
    python_callable=create_visualization_with_beam,
    dag=dag,
)


# function for 6. Delete the CSV file
def delete_csv_file():
    os.remove('/path/to/extracted_file.csv')


# function for 6. delete the CSV file
def delete_csv_file_with_beam():
    with beam.Pipeline(runner=DirectRunner()) as pipeline:
        # Delete CSV file
        delete_file = (
            pipeline
            | 'DeleteCSVFile' >> beam.Map(delete_csv_file)
        )


# 6. PythonOperator
delete_files_task = PythonOperator(
    task_id='delete_csv_file_with_beam',
    python_callable=delete_csv_file_with_beam,
    dag=dag,
)


# Setting task dependencies
file_sensor_task >> unzip_task >> extract_contents_task >> compute_averages_task >> create_plots_task >> delete_files_task
