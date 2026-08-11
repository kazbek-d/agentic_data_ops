#Используем ультралегкий и безопасный образ Python для аналитических скриптов
FROM python:3.11-slim

#Задаем рабочую директорию внутри контейнера
WORKDIR /app

#Отключаем буферизацию логов Python для моментального стриминга логов в докер
ENV PYTHONUNBUFFERED=1

#Копируем файл зависимостей
COPY requirements.txt .

#Устанавливаем библиотеки без кэширования для уменьшения веса контейнера
RUN pip install --no-cache-dir -r requirements.txt

#Копируем сам скрипт индексации
COPY rag_indexer.py .

#Точка входа запускает только индексацию
CMD ["python", "rag_indexer.py"]