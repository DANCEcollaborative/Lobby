FROM python:3.11.4

# Set the working directory in the container
WORKDIR /lobby

# Copy the current directory contents into the container at /app
COPY . /lobby

# Install the required packages
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which your Flask app runs (change this if your app uses a different port)
EXPOSE 5000

# Set the environment variable to run the Flask app
ENV FLASK_APP=lobby.py

# Start the Flask app
#CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
#CMD ["python", "-u", "-m", "gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby:app", "--worker-class", "eventlet"]
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "lobby:app", "--worker-class", "eventlet"]