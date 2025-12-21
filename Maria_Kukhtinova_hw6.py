from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys

def check_calculations_2_table():
    """Просто проверяем таблицу и выводим сообщение"""
    hook = PostgresHook(postgres_conn_id='postgres')
    
    result = hook.get_first("SELECT COUNT(*) FROM top_5_customers")
    
    if result[0] == 0:
        print("Ошибка! Результаты вычисления задачи 2 пустые!", file = sys.stderr)
    else:
        print(f"Результаты вычисления задачи 2 содержат {result[0]} строк - всё хорошо")

def check_calculations_1_table():
    """Просто проверяем таблицу и выводим сообщение"""
    hook = PostgresHook(postgres_conn_id='postgres')
    
    result = hook.get_first("SELECT COUNT(*) FROM top_3_min_max_customers")
    
    if result[0] == 0:
        print("Ошибка! Результаты вычисления задачи 1 пустые!", file = sys.stderr)
    else:
        print(f"Результаты вычисления задачи 1 содержат {result[0]} строк - всё хорошо")

def dag_success_message():
    print("DAG выполнен успешно!")

def dag_failure_message():
    print("DAG упал с ошибкой, какие-то задачи не выполнены")


default_args = {
    'owner': 'Maria',
    'start_date': datetime(2025, 12, 21),
    'retries': 1,
}


with DAG(
    'simple_data_loader',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    on_success_callback=dag_success_message,  
    on_failure_callback=dag_failure_message,
    tags = ['data_load'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Создание таблиц
    create_customer = PostgresOperator(
        task_id='create_customer_table',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS customer (
            customer_id INT4 PRIMARY KEY
            ,first_name VARCHAR(128) NOT NULL
            ,last_name VARCHAR(128)
            ,gender VARCHAR(128) NOT NULL
            ,DOB DATE
            ,job_title VARCHAR(128)
            ,job_industry_category VARCHAR(128)
            ,wealth_segment VARCHAR(128) NOT NULL
            ,deceased_indicator VARCHAR(128) NOT NULL
            ,owns_car VARCHAR(128) NOT NULL
            ,address VARCHAR(128) NOT NULL
            ,postcode VARCHAR(128) NOT NULL
            ,state VARCHAR(128) NOT NULL
            ,country VARCHAR(128) NOT NULL
            ,property_valuation INT2 NOT NULL
        );
        '''
    )
    
    create_product = PostgresOperator(
        task_id='create_product_table',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS product (
            product_id INT4 NOT NULL
            ,brand VARCHAR(128)
            ,product_line VARCHAR(128)
            ,product_class VARCHAR(128)
            ,product_size VARCHAR(128)
            ,list_price FLOAT4 NOT NULL
            ,standard_cost FLOAT4
        );
        '''
    )
    
    create_orders = PostgresOperator(
        task_id='create_orders_table',
        postgres_conn_id='postgres',
        sql='''
        --DROP TABLE IF EXISTS orders;
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT4 PRIMARY KEY
            ,customer_id INT4 NOT NULL
            ,order_date DATE NOT NULL
            ,online_order TEXT
            ,order_status VARCHAR(128) NOT NULL
        );
        '''
    )
    
    create_order_items = PostgresOperator(
        task_id='create_order_items_table',
        postgres_conn_id='postgres',
        sql='''
        --DROP TABLE IF EXISTS order_items;
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INT4 PRIMARY KEY
            ,order_id INT4 NOT NULL
            ,product_id INT4 NOT NULL
            ,quantity FLOAT NOT NULL
            ,item_list_price_at_sale FLOAT4 NOT NULL
            ,item_standard_cost_at_sale FLOAT4
        );
        '''
    )

    truncate_customer = PostgresOperator(
        task_id='truncate_customer_data',
        postgres_conn_id='postgres',
        sql="""
            TRUNCATE TABLE customer;
        """
    )

    truncate_product = PostgresOperator(
        task_id='truncate_product_data',
        postgres_conn_id='postgres',
        sql="""
            TRUNCATE TABLE product;
        """
    )

    truncate_orders = PostgresOperator(
        task_id='truncate_orders_data',
        postgres_conn_id='postgres',
        sql="""
            TRUNCATE TABLE orders;
        """
    )

    truncate_order_items = PostgresOperator(
        task_id='truncate_order_items_data',
        postgres_conn_id='postgres',
        sql="""
            TRUNCATE TABLE order_items;
        """
    )

    load_customer = PostgresOperator(
        task_id='load_customer_data',
        postgres_conn_id='postgres',
        sql="""
            COPY customer FROM '/opt/airflow/data/customer.csv'
            DELIMITER ';' CSV HEADER;
        """
    )

    load_product = PostgresOperator(
        task_id='load_product_data',
        postgres_conn_id='postgres',
        sql="""
            COPY product FROM '/opt/airflow/data/product.csv'
            DELIMITER ',' CSV HEADER;
        """
    )

    load_orders = PostgresOperator(
        task_id='load_orders_data',
        postgres_conn_id='postgres',
        sql="""
            COPY orders FROM '/opt/airflow/data/orders.csv'
            DELIMITER ',' CSV HEADER;
        """
    )

    load_order_items = PostgresOperator(
        task_id='load_order_items_data',
        postgres_conn_id='postgres',
        sql="""
            COPY order_items FROM '/opt/airflow/data/order_items.csv'
            DELIMITER ',' CSV HEADER;
        """
    )


    calculations_1 = PostgresOperator(

        #Найти имена и фамилии клиентов с ТОП-3 минимальной и ТОП-3 максимальной суммой 
        #транзакций за весь период (учесть клиентов, у которых нет заказов).
  
        task_id='calculations_1',
        postgres_conn_id='postgres',
        sql="""
            drop table if exists top_3_min_max_customers;

            create table top_3_min_max_customers as
            with raw_rating as (
            (select 
                customer.first_name,
                customer.last_name,
                customer.customer_id,
                coalesce(sum(order_items.item_list_price_at_sale * order_items.quantity), 0) as total_income
            from customer 
                left join orders on customer.customer_id = orders.customer_id
                left join order_items on orders.order_id = order_items.order_id
            group by customer.customer_id, customer.first_name, customer.last_name
            order by total_income asc
            limit 3)
            
            union all
            
            -- Клиенты с наибольшими покупками (включая нулевые)
            (select 
                customer.first_name,
                customer.last_name,
                customer.customer_id,
                coalesce(sum(order_items.item_list_price_at_sale * order_items.quantity), 0) as total_income
            from customer 
                left join orders on customer.customer_id = orders.customer_id
                left join order_items on orders.order_id = order_items.order_id
            group by customer.customer_id, customer.first_name, customer.last_name
            order by total_income desc
            limit 3)
        )
        select 
            first_name,
            last_name,
            customer_id
        from raw_rating;

            COPY top_3_min_max_customers 
            TO '/opt/airflow/data/top_customers_by_income.csv'
            WITH (FORMAT CSV, HEADER true, DELIMITER ',')
        """
    )

    calculations_2 = PostgresOperator(

        #Найти ТОП-5 клиентов (по общему доходу) в каждом сегменте благосостояния 
        #(wealth_segment). Вывести имя, фамилию, сегмент и общий доход. 
        #Если в сегменте менее 5 клиентов, вывести всех.
  
        task_id='calculations_2',
        postgres_conn_id='postgres',
        sql="""
            drop table if exists top_5_customers;

            create table top_5_customers as
            with income as (
            select 
                customer.first_name
                ,customer.last_name
                -- вдруг список кастомеров в разных таблицах разный:
                ,coalesce(customer.customer_id, orders.customer_id) as customer_id
                ,wealth_segment
                ,coalesce(sum(order_items.item_list_price_at_sale * order_items.quantity),0) as total_income
            from order_items 
                left join orders on order_items.order_id = orders.order_id
                left join customer on orders.customer_id = customer.customer_id
            where coalesce(customer.customer_id, orders.customer_id)  is not null
                and wealth_segment  is not null
            group by 1,2,3,4
            )
            select *
            from (
            select 
                *
                ,row_number() over (partition by wealth_segment order by total_income desc) as place_within_segment
            from income ) t
            where place_within_segment <=5;

            COPY top_5_customers 
            TO '/opt/airflow/data/top_customers_by_segment.csv'
            WITH (FORMAT CSV, HEADER true, DELIMITER ',')
        """
    )

    check_calculations_2= PythonOperator(
    task_id='check_calculations_2',
    python_callable=check_calculations_2_table,
)

    check_calculations_1= PythonOperator(
    task_id='check_calculations_1',
    python_callable=check_calculations_1_table,
)
    
    end = DummyOperator(task_id='end')

    start >> [create_customer, create_product, create_orders, create_order_items]
    create_customer >> truncate_customer >> load_customer
    create_product >> truncate_product >> load_product
    create_orders  >> truncate_orders >> load_orders
    create_order_items  >> truncate_order_items >> load_order_items
    [load_customer, load_product, load_orders, load_order_items] >> calculations_1 >> check_calculations_1
    [load_customer, load_product, load_orders, load_order_items] >> calculations_2 >> check_calculations_2
    [check_calculations_1, check_calculations_2] >> end