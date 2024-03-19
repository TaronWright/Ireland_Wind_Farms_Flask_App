FROM python:3.12

# set the working directory
WORKDIR /flaskapp

# install dependencies
COPY ./requirements.txt /flaskapp
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy your Flask application code
COPY . .

EXPOSE 5000

CMD ["python",  "wsgi.py"]


