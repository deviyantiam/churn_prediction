# Use the official Python 3.8 image
FROM python:3.8
RUN apt-get update && \
    apt-get install -y git bash && \
    apt-get clean
# Set the working directory inside the container
WORKDIR /app

# Copy the contents of the churn folder into the container at /app
COPY . /app

EXPOSE 8080
# Install any dependencies required by your Python application
RUN pip install -r requirements.txt

# Define the command to run your Python application
CMD ["python", "app.py"]
