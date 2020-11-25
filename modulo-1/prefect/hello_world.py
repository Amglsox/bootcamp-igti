import prefect
from prefect import task, Flow

@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

with Flow("Hello World") as flow:
    hello_task()

flow.register(project_name='Hello World')
flow.run_agent()