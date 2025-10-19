FROM python:3.11-slim

# Install packages one by one to avoid timeout
RUN pip install --no-cache-dir --timeout 300 kafka-python
RUN pip install --no-cache-dir --timeout 300 python-dotenv
RUN pip install --no-cache-dir --timeout 300 pandas
RUN pip install --no-cache-dir --timeout 300 faker
RUN pip install --no-cache-dir --timeout 300 snowflake-connector-python
RUN pip install --no-cache-dir --timeout 300 streamlit

WORKDIR /app
COPY . /app

CMD ["bash"]