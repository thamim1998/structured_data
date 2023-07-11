from airflow.decorators import dag, task
from datetime import datetime
import json

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)

def exc1():

    @task
    def read_users() -> str:
        users = []
        with open('users.json') as file:
            data = json.load(file)
            for item in data:
                user = User(item['id'], item['name'], item['city'], item['school'], item['age'], item['is_teacher'])
                print(vars(user))
                users.append(user)
        return json.dump(users)

    @task
    def print_users(users: str) -> str:
        user_list = json.load(users)
        for user in user_list:
            obj = User(user['id'], user['name'], user['city'], user['school'], user['age'], user['is_teacher'])
            print(vars(obj))

    name = read_users()
    hello = print_users(name)

_ = hello_with_passing_data_dag()


class User:
    def __init__(self, id, name, city, school, age, is_teacher):
        self.id = id
        self.name = name
        self.city = city
        self.school = school
        self.age = age
        self.is_teacher = is_teacher
