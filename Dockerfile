FROM python:3.7-slim

# Copy requirements to container
COPY ./requirements.txt .requirements.txt

# Install dependencies
RUN pip install -r .requirements.txt

# Create directory for application
RUN mkdir -p /home/ETL

#Copy source code to container
COPY . /home/ETL

#switch to working directory
RUN ls /home/ETL/*

# Run application
CMD ["python","/home/ETL/main.py"]