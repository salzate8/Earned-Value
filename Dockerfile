FROM --platform=linux/amd64 python:3.10
ENV DASH_DEBUG_MODE True
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY . .
RUN pip install --upgrade pip
RUN pip install --user -r requirements.txt
EXPOSE 80
CMD ["python", "models/plot.py"]

