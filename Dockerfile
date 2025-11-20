FROM python:3.11.4

# Set the working directory in the container
WORKDIR /lobby

# Copy the current directory contents into the container at /lobby
COPY . /lobby

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt
apt-get update && apt-get install -y ca-certificates

# Expose the port on which the Flask app will run
EXPOSE 5000

# Set the environment variable to run the Flask app
ENV FLASK_APP=lobby.py

# Start gunicorn with the Flask app 'lobby'
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby:app", "--worker-class", "eventlet", "--log-level", "debug"]
# CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby-direct:app", "--worker-class", "eventlet"]