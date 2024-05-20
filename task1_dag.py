from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import random
import os
import zipfile


year =  1901 # Change the year
num_files = 1  # Change the required number of files
required_location = '/home/saketh/Desktop/assignment2/required_location' # Change the required location in which the archive file is placed


# Function to select random files
def select_random_files(num_files, **kwargs): #year, num_files):
    
    with open('dags/page.html', 'r') as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all links in the HTML file
    links = soup.find_all('a')
    
    # Extract href attribute from each link
    csv_links = []
    for link in links:
        href = link.get('href')
        # Check if the link ends with '.csv'
        if href.endswith('.csv'):
            csv_links.append(href)

    # Select random files
    selected_files = random.sample(csv_links, num_files)

    # Write selected files to a text file
    with open('selected_files.txt', 'w') as f:
        for item in selected_files:
            f.write("%s\n" % item)

    return selected_files


def fetch_files():

    csv_links = []
    
    with open('selected_files.txt', 'r') as file:
        for line in file:
            # Remove leading/trailing whitespace and newline characters
            csv_file_name = line.strip()
            # Append the CSV file name to the list
            csv_links.append(csv_file_name)

    #print(csv_links)

    os.makedirs('selected_files')

    for link in csv_links:
        url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/1901/" + link 
        output_file = os.getcwd() + "/selected_files/" + link
        command = f"wget -O {output_file} {url}"
        os.system(command)
    
    return


# Function to zip fetched files
def zip_files():
    files_dir = os.getcwd() + "/selected_files/" # Adjust this to the directory where files are downloaded
    files_to_zip = os.listdir(files_dir)
    
    with zipfile.ZipFile(f"{os.getcwd()}/archive.zip", 'w') as zipf:
        for file in files_to_zip:
            zipf.write(os.path.join(files_dir, file), arcname=file)

    return


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 2),
    'retries': 1,
    'retry_delay':timedelta(minutes=2)
}

dag = DAG(
    dag_id='task1_dag',
    default_args=default_args,
    description='An example DAG using wget',
    schedule_interval=None,
)

# 1. Fetch the page containing the location wise datasets
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=f'wget -O {os.getcwd()}/dags/page.html https://www.ncei.noaa.gov/data/local-climatological-data/access/1901',
    dag=dag,
)

# 2. Select data files randomly
select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    op_kwargs={'num_files': 1},
    provide_context=True,
    dag=dag,
)

# 3. Fetch selected files
fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_files,
    #op_kwargs={},
    provide_context=True,
    dag=dag,
)

# 4. Zip fetched files
zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    provide_context=True,
    dag=dag,
)

# 5. Move the archive to the required location
move_archive_task = BashOperator(
    task_id='move_archive',
    bash_command=f'mv {os.getcwd()}/archive.zip {required_location}', 
)

fetch_page_task>>select_files_task>>fetch_files_task>>zip_files_task>>move_archive_task    
