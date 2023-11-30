FROM python:3.10
ENV DASH_DEBUG_MODE True
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY . .
RUN pip install --upgrade pip
RUN pip install --user -r requirements.txt
EXPOSE 8050
CMD ["python", "models/plot.py"]

