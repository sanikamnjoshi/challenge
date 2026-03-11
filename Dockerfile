FROM public.ecr.aws/lambda/python:3.11

RUN yum install -y java-17-amazon-corretto-devel

WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# --no-cache-dir to not bloat my docker image with installation cache

ENV SPARK_HOME=/var/lang/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$SPARK_HOME/bin

# explicitly set the python version to use for pyspark
ENV PYSPARK_PYTHON=/var/lang/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/var/lang/bin/python3.11

COPY src/spark_process.py .

CMD [ "spark_process.handler" ]