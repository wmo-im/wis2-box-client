FROM python:3.7-alpine
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt
COPY . /app
WORKDIR /app
CMD ["python", "consume.py"]