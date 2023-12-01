FROM python:3.10-alpine3.18

# Set working directory
WORKDIR /sparkplug-node

# Copy requirements and install dependencies
COPY requirements.txt /sparkplug-node/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
COPY ./sparkplug_node_app /sparkplug-node/sparkplug_node_app/
COPY ./run.py /sparkplug-node/

# Run run.py when the container launches
CMD ["python", "-m", "sparkplug_node_app"]