FROM python:3.11.4

# Set the working directory in the container
WORKDIR /lobby

# Install CA certificates to fix SSL verification errors
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /lobby
COPY . /lobby

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Update certifi to ensure Python has the latest CA bundle
RUN pip install --upgrade certifi

# Expose the port on which the Flask app will run
EXPOSE 5000

# Set the environment variable to run the Flask app
ENV FLASK_APP=lobby.py

# Start gunicorn with the Flask app 'lobby'
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby:app", "--worker-class", "eventlet", "--log-level", "debug"]
# CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby-direct:app", "--worker-class", "eventlet"]