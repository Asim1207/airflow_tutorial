try:
    # Import necessary modules
    from datetime import timedelta
    from Airflow.scripts.dags.airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("imports are ok!!")
except Exception as e:
    # Print error message if imports fail
    print("Error  {} ".format(e))


def first_function(**context):
    # Print a message indicating the first function is running
    print("first function runs")
    # Push a value to XCom for inter-task communication
    context['ti'].xcom_push(key='mykey', value="first function says Hello ")


def second_function(**context):
    # Pull the value pushed by the first function from XCom
    instance = context.get("ti").xcom_pull(key="mykey")
    # Create a DataFrame with sample data
    data = [{"name":"Asim","title":"Data Scientist"}, { "name":"Sohail","title":"Machine learning engineer"},]
    df = pd.DataFrame(data=data)
    # Print the DataFrame
    print('-'*22)
    print(df.head())
    print('-'*22)

    # Print a message indicating the second function is running and display the value from the first function
    print("I am in second_function got value :{} from Function 1  ".format(instance))


def third_function(**context):
    # Print a message indicating the third function is running
    print("third function runs")
    # Perform some additional task
    print("Performing additional task in third_function")

# Defining the DAG
with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    # Define the first task using PythonOperator
    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
        provide_context=True,
        op_kwargs={"name":"Asim Sohail"}
    )

    # Define the second task using PythonOperator
    second_function = PythonOperator(
        task_id="second_function",
        python_callable=second_function,
        provide_context=True,
    )

    # Define the third task using PythonOperator
    third_function = PythonOperator(
        task_id="third_function",
        python_callable=third_function,
        provide_context=True,
    )

# Set the task dependencies: first_function and third_function run in parallel, and both must complete before second_function runs
[first_function, third_function] >> second_function
